base on the trunk of Apace Avro 1.6.2, add some feature as follow:
1.modify some bug for use,such as org.apache.avro.reflect.ReflectData
Tips: the bug fix file at src/topsoft folder, use the same package and class name as the old file.
This have tested by more than ten hundred million data.
2.Add some support for mapreduce job, everyone can use it as MapFile and SequenceFile.
AvroPairInputFormat->SequenceFileInputFormat
AvroPairOutputFormat->SequenceFileOutputFormat
AvroMapOutputFormat->MapFileOutputFormat
MapAvroFile->MapFile
3.Add some classes use for special scene, such as MultithreadedBlockMapper,MultipleOutputs,RollOutputs...etc.