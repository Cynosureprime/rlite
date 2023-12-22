# rlite - lightweight version of rling

A simplistic version of the [rling utility](https://github.com/Cynosureprime/rling), this tool is designed to help users sort, de-duplicate lists against one another.  Compared to **rling**, **rlite** lacks some advanced features, though makes up for it by removing the need for extra dependencies and bringing along with it some speedups. While this is not a replacement, it is an alternative. Depending on the workload there may be some efficiency gains and regressions in others compared to **rling**.

## General information

This tool originally began when it was written in parallel with rling to see who would be able to be faster in de-duplicating lists against one another.  Since then it has undergone many changes to take it to where it is now. **rlite** shares a similar threading framework and also contains some code from **rling**, though you will notice it also handles the threading workloads quite differently. **rlite** was developed to push the envelope of multi-threaded computing and demonstrates this with it's ability push system resources including disk read/write operations, processing cores and also memory usage.

## Usage examples

**Sort** and **de-duplicate** the *input.txt* file, write the output to *output.txt*
```
rlite input.txt -o output.txt
```
**Remove** all the **common** lines in *bigfile1.txt* & *somefile.txt* from *input.txt* and write it to stdout
```
rlite input.txt bigfile.txt somefile.txt 
```
**Remove** all the **common** lines in *verybigfile1.txt* & *somefile.txt* from *input.txt*, assume the *input.txt* is **sorted** with the **-p switch** and write it to stdout
```
rlite input.txt verybigfile.txt somefile.txt -p
```
**Keep** the **common** lines from addresses.txt & streets.txt, redirect the stdout to a file
```
rlite input.txt streets.txt addresses.txt -c >> output.txt 
```
## Inspiration

"Key inspiration for this project came from tychotithonus. He suggested the project, and this quickly developed into a "who's smaller?" measuring contest of some kind between Waffle, blazer and hops." [see rling](https://github.com/Cynosureprime/rling)

Developed with technologies including yarn threads, roaring bitmaps, qsort_mt and xxhash, Written to be cross-compiled without external library dependencies.  

This could not have been possible without the help of Waffle and hops

## Contact

Created by blazer
