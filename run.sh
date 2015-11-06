#!/usr/bin/env bash

cd src
# I'll execute my programs, with the input directory tweet_input and output the files in the directory tweet_output
echo '###########################################'
echo '# Now begin single code sequential method #'
echo '###########################################'

python code_challenge.py ../tweet_input/tweets.txt ../tweet_output/ft1.txt ../tweet_output/ft2.txt

echo -e "\n"
echo -e "\n"
echo -e "\n"
echo '########################################'
echo '# Now begin hadoop parallelized method #'
echo '########################################'

python code_challenge_hadoop.py ../tweet_input/tweets.txt ../tweet_output/ft1_hadoop.txt ../tweet_output/unicode_count.txt ../tweet_output/ft2_hadoop.txt

rm -rf count_result

echo -e "\n"
echo -e "\n"
echo -e "\n"
echo '##########################################'
echo '# Now begin hive data warehouse building #'
echo '##########################################'

python code_challenge_hive.py ../tweet_input/tweets.txt ../tweet_output/ft1_hadoop.txt ../tweet_output/unicode_count.txt

rm -rf count_result



