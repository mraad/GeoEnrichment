hadoop fs -rm -skipTrash data.tsv
awk -f data.awk | hadoop fs -put - data.tsv