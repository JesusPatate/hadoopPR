lines = load '/pagerank/iter0/*.crawl' using PigStorage('\t') as (sourcepage:chararray, outlinks:{link:(url:chararray)});
--lines_pr = foreach lines generate 1.0 as pagerank, sourcepage, outlinks;    ------------------------------------------------------ => PAGERANK, PAGE, {OUTLINK} 

fractions = foreach lines generate flatten(outlinks), 1.0 / SIZE(outlinks) as ratio; 	 ------------------------------------------- => OUTLINK, FRAC
unrefenced_pages = foreach lines generate sourcepage, 0 as ratio; 		-------------------------------------------------------------=> PAGE 0 (pour ne pas perdre les pages non référencées) 
y = UNION fractions, unrefenced_pages;    -------------------------------------------------------------------------------------------=> PAGE, FRAC

z = GROUP y BY $0;    -------------------------------------------------------------------------------------------------------------- => PAGE, {(PAGE, FRAC)}
a = FOREACH z GENERATE group, 0.15 + 0.85 * SUM(y.ratio);   ------------------------------------------------------------------------ => PAGE, PAGERANK

dump a;
