package com.hirw.avro;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


public class AvroSerDe {

	private GenericRecord user1, user2, user3;
	private File file = new File("users.avro");
	Schema schema = null;
	
	public AvroSerDe() {
		URL url = getClass().getResource("user.avro.schema");
		try {
			schema = new Schema.Parser().parse(new File(url.getPath()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void populateUser() {
		
		
		user1 = new GenericData.Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);
		// Leave favorite color null

		user2 = new GenericData.Record(schema);
		user2.put("name", "Ben");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");
		        
		
		user3 = new GenericData.Record(schema);
		user3.put("name", "Charlie");
		user3.put("favorite_color", "blue");
		// Leave favorite number null
		
	}
	
	public void serializeToAvro() {
		
		// Serialize user1, user2 and user3 to disk
		File file = new File("users.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		try {
			dataFileWriter.create(schema, file);
			dataFileWriter.append(user1);
			dataFileWriter.append(user2);
			dataFileWriter.append(user3);
			dataFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}
	
	public void deserializeFromAvro() {
		
		// Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader;
		try {
			dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
			GenericRecord user = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println(user);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) {
		
		AvroSerDe avro = new AvroSerDe();
		avro.populateUser();
		avro.serializeToAvro();
		avro.deserializeFromAvro();
		
	}

}
