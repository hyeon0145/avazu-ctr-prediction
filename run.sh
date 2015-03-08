#!/bin/bash

# config

number_of_bins=1048576 # 2^20
learning_rate=0.01
number_of_reducer=4

hadoop="/home/hadoop/hadoop-1.2.1/bin/hadoop"
jar_filename="ctr-prediction.jar"
train_data_filename="train_rev2.4G"
predict_data_filename="test.csv"
result_data_filename="2011037365.res"

echo "Team 6: "
echo "- JHHyeon"
echo "- GYKim"
echo "- ESLee"

if [ ! -f $hadoop ]
then
  echo $hadoop "not found"
  exit 1
fi

if [ ! -f $jar_filename ]
then
  echo $jar_filename "not found"
  exit 1
fi

if [ ! -f $train_data_filename ]
then
  echo $train_data_filename "not found"
  exit 1
fi

if [ ! -f $predict_data_filename ]
then
  echo $predict_data_filename "not found"
  exit 1
fi

$hadoop fs -rmr ctr-prediction 2> /dev/null
$hadoop fs -rmr ctr-prediction-out 2> /dev/null

echo "Putting train data file"
$hadoop fs -mkdir ctr-prediction
$hadoop fs -put $train_data_filename ctr-prediction

echo "Traning data"
$hadoop jar $jar_filename train $number_of_bins $learning_rate $number_of_reducer ctr-prediction ctr-prediction-out

echo "Copying theta"
$hadoop fs -cat ctr-prediction-out/theta > theta

echo "Predicting data"
java -cp $jar_filename predictor.Predictor $number_of_bins theta $predict_data_filename > $result_data_filename

exit 0