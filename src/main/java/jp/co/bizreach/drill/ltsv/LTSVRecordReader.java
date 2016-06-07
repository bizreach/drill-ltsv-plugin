package jp.co.bizreach.drill.ltsv;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.List;

public class LTSVRecordReader extends AbstractRecordReader {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LTSVRecordReader.class);
    private static final int MAX_RECORDS_PER_BATCH = 8096;

    private String inputPath;
    private BufferedReader reader;
    private DrillBuf buffer;
    private VectorContainerWriter writer;
    private LTSVFormatPlugin.LTSVFormatConfig config;
    private int lineCount;

    public LTSVRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem,
                            List<SchemaPath> columns, LTSVFormatPlugin.LTSVFormatConfig config) throws OutOfMemoryException {
        try {
            FSDataInputStream fsStream = fileSystem.open(new Path(inputPath));
            this.inputPath = inputPath;
            this.lineCount = 0;
            this.reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), "UTF-8"));
            this.config = config;
            this.buffer = fragmentContext.getManagedBuffer();
            setColumns(columns);

        } catch(IOException e){
            logger.debug("LTSV Plugin: " + e.getMessage());
        }
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        this.writer = new VectorContainerWriter(output);
    }

    public int next() {
        this.writer.allocate();
        this.writer.reset();

        int recordCount = 0;

        try {
            BaseWriter.MapWriter map = this.writer.rootAsMap();
            String line = null;

            while(recordCount < MAX_RECORDS_PER_BATCH &&(line = this.reader.readLine()) != null){
                lineCount++;

                // Skip empty lines
                if(line.trim().length() == 0){
                    continue;
                }

                this.writer.setPosition(recordCount);
                map.start();

                String[] fields = line.split("\t");
                for(String field: fields){
                    int index = field.indexOf(":");
                    if(index <= 0) {
                        throw new ParseException(
                            "Invalid LTSV format: " + inputPath + "\n" + lineCount + ":" + line, 0
                        );
                    }

                    String fieldName  = field.substring(0, index);
                    String fieldValue = field.substring(index + 1);

                    byte[] bytes = fieldValue.getBytes("UTF-8");
                    this.buffer.setBytes(0, bytes, 0, bytes.length);
                    map.varChar(fieldName).writeVarChar(0, bytes.length, buffer);
                }

                map.end();
                recordCount++;
            }

            this.writer.setValueCount(recordCount);
            return recordCount;

        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    public void close() throws Exception {
        this.reader.close();
    }

}
