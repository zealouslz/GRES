package com.briup.bigdata.project.gres.step9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class UgxWritable implements WritableComparable<UgxWritable> , DBWritable {
    private Text uid;
    private Text gid;
    private IntWritable exp;

    public UgxWritable() {
        this.uid=new Text();
        this.gid=new Text();
        this.exp=new IntWritable();
    }

    @Override
    public int compareTo(UgxWritable o) {
        int uidComp=this.uid.compareTo(o.uid);
        int gidComp=this.gid.compareTo(o.gid);
        int expComp=this.exp.compareTo(o.exp);
        return -(expComp==0?(uidComp==0?gidComp:uidComp):expComp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UgxWritable that = (UgxWritable) o;
        return uid.equals(that.uid) &&
                gid.equals(that.gid) &&
                exp.equals(that.exp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, gid, exp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
          this.uid.write(out);
          this.gid.write(out);
          this.exp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid.readFields(in);
        this.gid.readFields(in);
        this.exp.readFields(in);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
      statement.setString(1,this.uid.toString());
      statement.setString(2,this.gid.toString());
      statement.setInt(3,this.exp.get());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
          this.uid.set(resultSet.getString(1));
          this.gid.set(resultSet.getString(2));
          this.exp.set(resultSet.getInt(3));
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid.set(uid.toString());
    }
    public void setUid(String uid) {
        this.uid.set(uid);
    }
    public Text getGid() {
        return gid;
    }

    public void setGid(Text gid) {
        this.gid.set(gid.toString());
    }
    public void setGid(String gid) {
        this.gid.set(gid);
    }
    public IntWritable getExp() {
        return exp;
    }

    public void setExp(IntWritable exp) {
        this.exp.set(exp.get());
    }
    public void setExp(int exp) {
        this.exp.set(exp);
    }

    @Override
    public String toString() {
        return  this.uid.toString()+"\t"+this.gid.toString()+"\t"+this.exp.get();
    }
}
