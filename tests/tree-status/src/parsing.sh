#!/bin/bash
file=$1

missing_seq=$(grep MissingSeq $file)
echo Missing sequences by pubkey summary > Summary.log
echo " " >> Summary.log
echo The keys with missing sequences are: >> Summary.log
echo " " >> Summary.log
echo $missing_seq | sed -E 's/(})\ /\1\n/g' | awk -F, '{print $1}' | uniq > pubkeys.log

while read -r line;
do
    pubkeys+=("$line")
    echo $line >> Summary.log
done < pubkeys.log

pubkeys_count=${#pubkeys[@]}

echo " " >> Summary.log

echo $missing_seq | sed -E 's/(})\ /\1\n/g' > missing_seq1.log

# echo Parse missing sequences
for pubkey in ${pubkeys[*]}
do
    missing_seq_count=$(echo $missing_seq | sed -E 's/(})\ /\1\n/g' | grep $pubkey | wc -l)
    echo $pubkey has $missing_seq_count missing sequences >> Summary.log
    echo Missing sequences are: >> Summary.log
    echo $missing_seq | sed -E 's/(})\ /\1\n/g' | grep $pubkey | awk  '{print $5}' > missing_seq2.log
    while read -r line;
    do
        echo $line >> Summary.log
    done < missing_seq2.log
    echo " " >> Summary.log
    total_missing_seq_count=$(($total_missing_seq_count+$missing_seq_count))
done

echo " " >> Summary.log
echo Total number of keys with missing sequences: $pubkeys_count >> Summary.log
echo " " >> Summary.log
echo Total number of missing sequences: $total_missing_seq_count >> Summary.log

rm missing_seq2.log missing_seq1.log pubkeys.log
