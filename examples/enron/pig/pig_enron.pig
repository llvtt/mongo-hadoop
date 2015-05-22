--Change these jar locations to point to the correct locations/version on your system.
REGISTER /home/luke/code/mongo-hadoop/core/build/libs/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER /home/luke/code/mongo-hadoop/pig/build/libs/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
REGISTER /home/luke/local/jars/mongo-java-driver-3.0.1.jar

-- raw = LOAD 'file:///tmp/enron_mail/messages.bson' using com.mongodb.hadoop.pig.BSONLoader('','headers:[]') ;
raw = LOAD 'mongodb://localhost:27017/mongo_hadoop.messages'
      USING com.mongodb.hadoop.pig.MongoLoader('headers:[]', '');
send_recip = FOREACH raw GENERATE $0#'From' as from, $0#'To' as to;
send_recip_filtered = FILTER send_recip BY to IS NOT NULL;
send_recip_split = FOREACH send_recip_filtered GENERATE from as from, FLATTEN(TOKENIZE(to)) as to;
send_recip_split_trimmed = FOREACH send_recip_split GENERATE from as from, TRIM(to) as to;
send_recip_grouped = GROUP send_recip_split_trimmed BY (from, to);
send_recip_counted = FOREACH send_recip_grouped GENERATE group, COUNT($1) as count;
STORE send_recip_counted
      INTO 'mongodb://localhost:27017/mongo_hadoop.pig_message_pairs'
      USING com.mongodb.hadoop.pig.MongoStorage;
