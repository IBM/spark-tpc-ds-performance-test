#!/bin/perl

my $debug = 0;

#print "INFO: Generating TPC-DS Qualification Queries...\n";
read_qualification_parameters();
gen_qualification_queries();
#print "INFO: Completed generating TPC-DS Qualification Queries...\n";

sub read_qualification_parameters
{
  # my $appendixbfile = "appendixb.txt";
  my $appendixbfile = $ENV{'TPCDS_ROOT_DIR'} . "/src". "/properties" . "/appendixb.txt";
  my $query;
  my $template;

  $debug && print "DEBUG: Reading $appendixbfile...\n";

  open FH, $appendixbfile;
  while (<FH>)
  {
    chomp;

    if (/^$/) { next; }

    if (/tpl$/)
    {
      $template = $_;

      $query = $template;
      $query =~ s/\D+//g;

      $debug && print "DEBUG: Found template $template (query $query)\n";

      $qualparams{$template}->{'_QUERY'} = $query;
      $qualparams{$template}->{'_TEMPLATE'} = $template;

      next;
    }

    my ($key, $val) = split(/=/);
    $debug && print "DEBUG: Found key/value pair $key => $val\n";
    $qualparams{$template}->{$key} = $val;
  }

  close FH;

  my $dsgenDir    = $ENV{'TPCDS_ROOT_DIR'} . "/src". "/toolkit";
  my $toolsDir    = $dsgenDir . "/tools";
  my $tplDir      = $ENV{'TPCDS_WORK_DIR'};

  my $dialectfile = $tplDir. "/spark.tpl";

  $debug && print "DEBUG: Reading $dialectfile...\n";

  open FH, $dialectfile;
  while (<FH>)
  {
    if (/define/)
    {
omp;
      s/define//g;
      s/;//g;
      my ($key, $val) = split(/=/);
      $key =~ s/\s+//g; # trim all spaces from key
      $val =~ s/^\s+//g; # trim leading spaces from val
      $val =~ s/\s+$//g; # trim trailing spaces from val

      $debug && print "DEBUG: Found key/value pair $key => $val\n";

      # Special Handling for Limit-related Keys
      if ($key =~ /LIMIT/)
      {
        if ($key =~ /__/) { $key =~ s/__/_/g; }
        if ($val =~ /%d/) { $val =~ s/%d/\[_LIMIT\]/g; }
        if ($val =~ /\"/) { $val =~ s/\"//g; }
      }

      # Special Handling for _BEGIN/_END
      if ($key eq "_BEGIN" || $key eq "_END")
      {
        my @temp = split(/\+/, $val);
        my $newval = "";
        foreach my $val (@temp)
        {
          $val =~ s/\"\s+/"/g;  # trailing whitespace
          $val =~ s/\s+\"/"/g;  # leading whitespace
          $val =~ s/\"//g;      # quotes
          $newval .= $val;
        }
        $val = $newval;
      }

      $debug && print "DEBUG: Found key/value pair $key => $val\n";

      $qualparams{'GLOBAL'}->{$key} = $val;
    }
  }
  close FH;

  $qualparams{'GLOBAL'}->{'_SEED'} = 'QUALIFICATION';
  $qualparams{'GLOBAL'}->{'_STREAM'} = '0';
}

sub gen_qualification_queries
{
  my $dsgenDir    = $ENV{'TPCDS_ROOT_DIR'} . "/src". "/toolkit";
  my $toolsDir    = $dsgenDir . "/tools";
  my $tplDir      = $ENV{'TPCDS_WORK_DIR'};

  my $outdir = $ENV{'TPCDS_ROOT_DIR'} . "/genqueries";

  $debug && print "DEBUG: Using templates from $tplDir...\n";

  foreach my $infile (glob($tplDir . "/query*.tpl"))
  {
    $debug && print "DEBUG: Reading template file $infile...\n";

    my @data = ();
    push @data, "[_BEGIN]\n";
    open FH, $infile;
    push @data, <FH>;
    close FH;
    push @data, "[_END]\n";

    my $tpl = $infile;
    $tpl =~ s#.*/##g;

    my $qfmt = $tpl;
    $qfmt =~ s/\D+//g;
    $qfmt = sprintf("%02d", $qfmt);

    my $outfile = $outdir . "/query${qfmt}.sql";

    $debug && print "DEBUG: Generating query into file $outfile using template $tpl...\n";

    open FH, ">$outfile";

    my $indef = 0;

    foreach my $line (@data)
    {
      $debug && print "DEBUG: indef=$indef, Found line $line\n";


      # comments and blank lines
      if ($line =~ /^--/) { next; }
      if ($line =~ /^$/) { next; }
      if ($line =~ /^\s+$/) { next; }

      # NOTE: Due to TPC query template inconsistencies, we must use case-
      #       insensitive matching (queries 12, 39, 91, 92, 96, 98)
      # NOTE: Still an issue as of TPC-DS v2.3.0
      if ($line =~ /define/i)
      {
        $indef = 1;

        # LIMIT is the only parameter whose value we pull from the template.
        if ($line =~ /_LIMIT/)
        {
          $line =~ s/define//g;
          $line =~ s/;//g;
          $line =~ s/\s+//g;
          my ($key,$val) = split(/=/,$line);
          $qualparams{$tpl}->{$key} = $val;
          $indef = 0;
          next;
        }
      }

      # Since defines can be on multiple lines, we need to track whether
      # we are "in" a define and skip all lines until we hit a terminator.
      if ($indef)
      {
        if ($line =~ /;/) { $indef = 0; }
        next;
      }

      # must loop to get multiple replacements per line
      # parameters are enclosed in square brackets and contain uppercase letters, numbers, period and undercore.
      # NOTE: Due to TPC query template inconsistencies, we must use case-
      #       insensitive matching (query 66) and force keys to uppercase.
      #       We also allow spaces in keys (query 85) and strip them.
      # NOTE: Still an issue as of TPC-DS v2.3.0
      my $oldkey = "";
      while ($line =~ /\[([A-Z0-9_\. ]+)\]/i)
      {
        my $foundkey = $1;
        my $key = uc($foundkey);
        $key =~ s/\s+//g;

        my $val = $qualparams{$tpl}->{$key};
        if ($val eq "") { $val = $qualparams{'GLOBAL'}->{$key}; }

        if ($key eq $oldkey) { print "ERROR: Parameter $key not found, skipping.\n"; last; }
        if (($key !~ /(_BEGIN|_END|_LIMIT[ABC])/) && $val eq "") { print "ERROR: Value not found for param $key, skipping.\n"; last; }

        $debug && print "DEBUG: found key $key, replacing with value $val\n";

        # NOTE: It is valid to do global search/replace here.
        $line =~ s/\[$foundkey\]/$val/gi;

        $oldkey = $key;
      }

      print FH $line;
    }
    close FH;
  }
}
