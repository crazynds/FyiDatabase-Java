package engine.virtualization.record.manager.storage;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.file.streams.ReferenceReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.instances.GenericRecord;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class OptimizedFixedRecordStorage extends FixedRecordStorage{
    public OptimizedFixedRecordStorage(FileManager fm, RecordInterface ri, int sizeOfEachRecord) {
        super(fm, ri, sizeOfEachRecord);
    }

    public OptimizedFixedRecordStorage(FileManager fm, RecordInterface ri, int sizeOfEachRecord, int tempBufferSize) {
        super(fm, ri, sizeOfEachRecord, tempBufferSize);
    }


    @Override
    public void writeNew(List<Record> list) {
        TreeMap<BigInteger,byte[]> records = new TreeMap<BigInteger,byte[]>();
        byte[] data = null;

        GenericRecord buffer = new GenericRecord(new byte[sizeOfEachRecord]);
        long pos = 0;

        for (Record r:list) {
            if(r.size()==sizeOfEachRecord){
                data = r.getData();
                if(data.length<sizeOfEachRecord)throw new DataBaseException("FixedRecordStorage->writeNew","Tamanho passado no vetor de dados é menor que o informado na classe record");
            }else{
                data = new byte[sizeOfEachRecord];
                System.arraycopy(r.getData(),0,data,0,(r.size()<sizeOfEachRecord)?r.size():sizeOfEachRecord);
            }
            records.put(recordInterface.getExtractor().getPrimaryKey(r),data);
        }

        lock.writeLock().lock();
        try {
            pos = findStartInsert(records,buffer);
            WriteByteStream wbs = getWriteByteStream();
            insertLoop(records,buffer,wbs,pos);
            changed=true;
            flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private long findStartInsert(TreeMap<BigInteger,byte[]> records, GenericRecord buffer){
        long startPos;
        startPos = findRecordBinarySearch(records.firstKey(), 0, qtdOfRecords - 1, buffer);

        if(startPos > sizeOfBytesQtdRecords) {
            heap.read(startPos, buffer.getData(), 0, sizeOfEachRecord);
            while(recordInterface.getExtractor().isActiveRecord(buffer)==false && startPos>sizeOfBytesQtdRecords){
                startPos-=sizeOfEachRecord;
                heap.read(startPos, buffer.getData(), 0, sizeOfEachRecord);
            }
        }

        return (startPos-sizeOfBytesQtdRecords)/sizeOfEachRecord;
    }
    private void insertLoop(TreeMap<BigInteger,byte[]> records, GenericRecord buffer,WriteByteStream wbs,long pos){
        Map.Entry<BigInteger,byte[]> entry=null;
        LinkedList<byte[]> list = new LinkedList<>();
        LinkedList<BigInteger> listKey = new LinkedList<>();
        ReferenceReadByteStream reference = new ReferenceReadByteStream(heap,0);
        RecordInfoExtractor extractor = recordInterface.getExtractor();
        byte[] data = null;
        long readOffset= pos;
        long writeOffset = pos;

        while(readOffset<qtdOfRecords && !records.isEmpty()){
            long readPosition = getPositionOfRecord(readOffset);
            //Ao invez de fazer a leitura inteira do record, ler apenas os dados necessários e se
            // indentificado que vai ser necessário a leitura completa, ele le completamente
            reference.setOffset(readPosition);

            //heap.read(readPosition,sizeOfEachRecord,buffer.getData(),0);

            if(extractor.isActiveRecord(reference)) {
                long writePosition = getPositionOfRecord(writeOffset);
                BigInteger firstKey = records.firstKey();
                BigInteger buffPk = extractor.getPrimaryKey(reference);
                switch (firstKey.compareTo(buffPk)) {
                    case -1:
                        do{
                            entry = records.pollFirstEntry();
                            data = entry.getValue();
                            wbs.write(writePosition, data, sizeOfEachRecord);
                            recordInterface.updeteReference(entry.getKey(), writePosition);
                            writeOffset++;
                            writePosition+= sizeOfEachRecord;
                            /**
                             * Estrategia de otimização:
                             * Remover a verificaçõa writeOffset <= readOffset
                             * Enquanto a key alvo for menor do que a key na fila, vai escrevendo.
                             */
                        }while (
                                //writeOffset <= readOffset &&
                                !records.isEmpty() &&
                                records.firstKey().compareTo(buffPk) != 1);// -1 continue, 0 substitute

                        if(entry.getKey().compareTo(buffPk) == 0) {
                            if (writeOffset <= readOffset) {
                                writePosition = getPositionOfRecord(writeOffset);
                                while ((readOffset+1) - writeOffset > records.size()) {
                                    wbs.write(writePosition, invalidRecord.getData(), invalidRecord.size());
                                    writeOffset++;
                                    writePosition += sizeOfEachRecord;
                                }
                                wbs.write(readPosition,invalidRecord.getData(),invalidRecord.size());
                                writeOffset++;
                            }
                        }else{
                            if(writeOffset>readOffset) {
                                //Necessário para leitura tardia do dado em buffer
                                reference.read(0,buffer.getData(),0,sizeOfEachRecord);
                                records.putIfAbsent(buffPk, buffer.getData());
                                buffer.setData(data);
                            }else if(writeOffset<readOffset) {
                                //Necessário para leitura tardia do dado em buffer
                                reference.read(0,buffer.getData(),0,sizeOfEachRecord);
                                writePosition = getPositionOfRecord(writeOffset);
                                while (readOffset - writeOffset > records.size()) {
                                    wbs.write(writePosition, invalidRecord.getData(), invalidRecord.size());
                                    writeOffset++;
                                    writePosition+=sizeOfEachRecord;
                                }
                                data = buffer.getData();
                                wbs.write(writePosition, data, sizeOfEachRecord);
                                recordInterface.updeteReference(buffPk, writePosition);
                                writeOffset++;
                            }else{
                                writeOffset++;
                            }
                        }
                        break;
                    case 0:
                        entry = records.pollFirstEntry();
                        data = entry.getValue();
                        wbs.write(writePosition, data, sizeOfEachRecord);
                        recordInterface.updeteReference(entry.getKey(), writePosition);
                        writeOffset++;
                        break;
                    case 1:
                        if(writeOffset<readOffset) {
                            //Necessário para leitura tardia do dado em buffer
                            reference.read(0,buffer.getData(),0,sizeOfEachRecord);
                            writePosition = getPositionOfRecord(writeOffset);
                            while (readOffset - writeOffset > records.size()) {
                                wbs.write(writePosition, invalidRecord.getData(), invalidRecord.size());
                                writeOffset++;
                                writePosition+=sizeOfEachRecord;
                            }
                            data = buffer.getData();
                            wbs.write(writePosition, data, sizeOfEachRecord);
                            recordInterface.updeteReference(buffPk, writePosition);
                            writeOffset++;
                        }else if(writeOffset==readOffset){
                            writeOffset=readOffset+1;
                        }else{
                            //Necessário para leitura tardia do dado em buffer
                            reference.read(0,buffer.getData(),0,sizeOfEachRecord);
                            wbs.write(writePosition,buffer.getData(),sizeOfEachRecord);
                            recordInterface.updeteReference(buffPk, writePosition);
                            writeOffset++;
                        }
                        break;
                }
            }
            readOffset++;
        }
        while(readOffset<qtdOfRecords){
            long readPosition = getPositionOfRecord(readOffset);
            long writePosition = getPositionOfRecord(writeOffset);
            heap.read(readPosition,buffer.getData(),0,sizeOfEachRecord);
            wbs.write(writePosition,buffer.getData(),sizeOfEachRecord);
            writeOffset++;
            readOffset++;
        }
        while((entry = records.pollFirstEntry())!=null){
            long position = getPositionOfRecord(writeOffset);
            data = entry.getValue();
            wbs.write(position, data, sizeOfEachRecord);
            recordInterface.updeteReference(entry.getKey(), position);
            writeOffset++;
        }
        qtdOfRecords = (int)writeOffset;
    }
}
