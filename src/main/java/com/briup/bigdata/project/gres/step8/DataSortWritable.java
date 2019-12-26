package com.briup.bigdata.project.gres.step8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DataSortWritable implements WritableComparable<DataSortWritable> {
    private Text uid;
    private Text gid;
    private Text exp;

    public DataSortWritable() {
        this.uid=new Text();
        this.gid=new Text();
        this.exp=new Text();
    }

    @Override
    public int compareTo(DataSortWritable o) {
        int uidComp=uid.compareTo(o.uid);
        int gidComp = gid.compareTo(o.gid);
        int expComp = exp.compareTo(o.exp);
        return -(expComp==0?(uidComp==0?gidComp:uidComp):expComp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSortWritable that = (DataSortWritable) o;
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

    public Text getExp() {
        return exp;
    }

    public void setExp(Text exp) {
        this.exp.set(exp.toString());
    }
    public void setExp(String exp) {
        this.exp.set(exp);
    }

    @Override
    public String toString() {
        return this.uid.toString()+"\t"+this.gid.toString()+"\t"+this.exp.toString();
    }
}
