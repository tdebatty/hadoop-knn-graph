<?php

$out = "nndescent-30-recall.dat";

$lines = file("nndescent-30.dat");

$r = array();
foreach ($lines as $line) {
  $elements = explode("\t", $line);
  $it = $elements[0];
  $count = $elements[1];
  $recall = $count / 20000;
  $r[] = "$it\t$recall";
  
}

file_put_contents($out, implode(PHP_EOL,$r));
