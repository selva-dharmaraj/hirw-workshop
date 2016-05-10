import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

reader = DataFileReader(open("/hirw-workshop/fileformats/avro/part-r-00000.avro", "r"), DatumReader())
for user in reader:
    print user
reader.close()