package com.mapreduce.demo1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReceiveTable implements Writable,DBWritable{
	  private String id;

	  private int weight;
	  
	 
	  
	  public ReceiveTable(){

	  }
	  
	  public ReceiveTable(String id,int weight){
	 this.id = id;
	 this.weight = weight;
	  
	  }
	  
		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1,this.id);

			statement.setInt(2,this.weight);
			
		}
		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
		
			this.id = resultSet.getString(1);

			this.weight = resultSet.getInt(2);
			
			
		}
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeInt(weight);
			Text.writeString(out,this.id);

			
			
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readUTF();

			this.weight = in.readInt();
			
		
		}
   

}
