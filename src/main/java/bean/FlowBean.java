package bean;


import lombok.*;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class FlowBean implements WritableComparable<FlowBean> {

    private String useId;
    private long up_flow;
    private long d_flow;
    private long s_flow;

    public FlowBean(String userId, long u_flow, long d_flow) {
    }


    public void write(DataOutput out) throws IOException {

        out.writeUTF(useId);
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(s_flow);


    }

    public void readFields(DataInput in) throws IOException {

        useId = in.readUTF();
        up_flow = in.readLong();
        d_flow = in.readLong();
        s_flow = in.readLong();

    }

    public int compareTo(FlowBean o) {

        return s_flow > o.getS_flow() ? -1 : 1;//按总流量大小排序，从大到小
    }

}
