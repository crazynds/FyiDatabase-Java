package sgbd.prototype;

import engine.exceptions.DataBaseException;
import engine.util.Util;
import engine.virtualization.record.Record;
import engine.virtualization.record.instances.GenericRecord;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TranslatorApi {

    private final int headerSize;

    private final ArrayList<Column> columns;
    private final HashMap<Integer,Integer> headerPosition;

    private final int primaryKeySize;

    protected TranslatorApi(ArrayList<Column> columns){
        this.columns=columns;
        int headerSize = 1;
        this.headerPosition = new HashMap<>();
        /*
        0 - 256
                0-253 -> ja é o tamanho => 1 byte
                254 -> deve ler um short => 3 byes
                255 -> deve ler um int   => 5 bytes => unico pior caso
                5 bytes
                30 = nome -> 14 => 1 byte
                44 = email -> 20 => 1 byte
                64 = descricao -> 500 => 3 bytes
                //
                12 bytes
                int == 4 bytes
                [int 30,int 44,int 64]
                30 = nome -> 14
                44 = email -> 20
                64 = descricao -> 500
        */
        int aux=1;
        int sizePk = 0;
        for (Column c:columns){
            if(c.camBeNull()){
                this.headerPosition.put(columns.indexOf(c),(headerSize-1)*8+aux);
                aux++;
            }
            if(aux>=8){
                headerSize++;
                aux=0;
            }
            if(c.isPrimaryKey()){
                sizePk +=c.getSize();
            }
        }
        this.headerSize = headerSize;
        primaryKeySize = sizePk;
    }


    public BigInteger getPrimaryKey(RowData rw){
        BigInteger pk;
        ByteBuffer buffer = ByteBuffer.allocate(primaryKeySize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(Column c:columns){
            if(c.isPrimaryKey()){
                byte[] arr = rw.getData(c.getName());
                buffer.put(arr);
            }else{
                break;
            }
        }
        return Util.convertByteArrayToNumber(buffer.array());
    }

    public BigInteger getPrimaryKey(Record r){
        return Util.convertByteArrayToNumber(r.read(this.headerSize,primaryKeySize));
    }

    public void validateRowData(RowData rw){
        for(Column c: columns){
            byte[] data = rw.getData(c.getName());
            if(data == null){
                if(!c.camBeNull()){
                    throw new DataBaseException("RecordTranslateApi->convertToRecord","Coluna "+c.getName()+" não pode ser nula!");
                }else if(c.isPrimaryKey()){
                    throw new DataBaseException("RecordTranslateApi->convertToRecord","Coluna "+c.getName()+" não pode ser nula!");
                }
            }else{
                if(c.isDinamicSize()){
                    if(data.length>c.getSize()){
                        throw new DataBaseException("RecordTranslateApi->convertToRecord","Dado passado para a coluna "+c.getName()+" é maior que o limite: "+c.getSize());
                    }
                }else{
                    if(data.length!=c.getSize()){
                        throw new DataBaseException("RecordTranslateApi->convertToRecord","Dado passado para a coluna "+c.getName()+" é diferente do tamanho fixo: "+c.getSize());
                    }
                }
            }
        }
    }

    public RowData convertToRowData(Record r){
        for(Column c: columns){
        }
        return null;
    }
    public RowData convertToRowData(Record r, List<String> select){
        for(Column c: columns){
        }
        return null;
    }

    public Record convertToRecord(RowData rw){
        this.validateRowData(rw);
        byte[] header = new byte[this.headerSize];
        ArrayList<byte[]> dados = new ArrayList<>();
        int size = this.headerSize;
        for(Column c: columns){
            byte[] data = rw.getData(c.getName());
            if(data == null){
                if(c.camBeNull()){
                    int posHeader = headerPosition.get(columns.indexOf(c));
                    header[posHeader/8] |= 1<<(posHeader%8);
                }
            }else{
                if(c.isDinamicSize()){
                    ByteBuffer buff = ByteBuffer.allocate(4);
                    buff.order(ByteOrder.LITTLE_ENDIAN);
                    buff.putInt(data.length);
                    byte[] indice = buff.array();
                    dados.add(indice);
                    dados.add(data);
                    size+=indice.length;
                    size+=data.length;
                }else{
                    dados.add(data);
                    size+=data.length;
                }
            }
        }
        byte[] bufferRecord = new byte[size];
        int offset = this.headerSize;
        System.arraycopy(header,0,bufferRecord,0,header.length);
        for(byte[] data:dados){
            System.arraycopy(data,0,bufferRecord,offset,data.length);
            offset+=data.length;
        }
        return new GenericRecord(bufferRecord);
    }

}
