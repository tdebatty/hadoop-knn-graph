<?php

$values = file("number_of_strings_per_bucket");
$freqs = array();
foreach ($values as $value) {
  $freqs[round($value, -1)]++;
}

foreach ($freqs as $value => $count) {
  file_put_contents("freqs.dat", "$value\t$count\n", FILE_APPEND);
}
