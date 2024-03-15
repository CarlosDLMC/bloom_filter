export ENVYK=

kinit -kt /home/ingestdl00/ingestdl00.keytab ingestDL00@SGT.SGTECH.CORP

yesterday=$(date -d "yesterday" +%Y-%m-%d)
two_months_ago=$(date -d "2 months ago" +%Y-%m-%d)

echo "Yesterday's date: $yesterday"
echo "Date of 2 months ago: $two_months_ago"


nohup spark-submit \
  --driver-memory 6G \
  --executor-memory 3G \
  --master yarn \
  --deploy-mode cluster \
  --queue root.default \
  --py-files envypack.zip,date_utils.py,otherlibs.zip \
  --archives envypack.zip \
  --files .env \
  --conf spark.yarn.appMasterEnv.ENVYK=${ENVYK} \
  --conf spark.driver.maxResultSize=6G \
  main.py -sd $two_months_ago -ed $yesterday -p 0.005 > outputs/lanzando_mes_de_${two_months_ago}_a_${yesterday}.out &