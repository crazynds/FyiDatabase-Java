package sgbd.query.unaryop;

import engine.util.Util;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.agregation.AgregationOperation;

import java.math.BigInteger;
import java.util.*;

public class GroupOperator extends UnaryOperator {

    private String source,column;

    private BigInteger groupName;
    private Tuple actualTuple,lastTupleLoaded;
    List<AgregationOperation> agregationOperations;

    public GroupOperator(Operator op,String source, String column,List<AgregationOperation> agregationOperations) {
        super(new ExternalSortOperator(op,source,column,false));
        this.source = source;
        this.column = column;
        this.agregationOperations = agregationOperations;
    }

    @Override
    public void open() {
        super.open();
        lastTupleLoaded = null;
        actualTuple = null;
        groupName = null;
    }

    private Tuple getNextTuple(){
        if(actualTuple!=null)
            return actualTuple;
        while(operator.hasNext()){
            // Se não tem a prox tupla carregada, carrega ela
            if(lastTupleLoaded==null)
                lastTupleLoaded = operator.next();

            // Se a tupla de acumulador n existir, cria ela e já vincula a agregação atual
            if(actualTuple ==null){
                // Cria a tupla
                actualTuple = new Tuple();
                ComplexRowData rowGroup = new ComplexRowData();
                rowGroup.setData(column,lastTupleLoaded.getContent(source).getData(column),lastTupleLoaded.getContent(source).getMeta(column));
                // Seta nela o dado inicial
                actualTuple.setContent(source,rowGroup);
                groupName = Util.convertByteArrayToNumber(lastTupleLoaded.getContent(source).getData(column));
                // Chama a inicialização de todas as operações de agregações
                agregationOperations.stream().forEach(agregationOperation -> agregationOperation.initialize(actualTuple));
            }else if(Util.convertByteArrayToNumber(
                        lastTupleLoaded.getContent(source).getData(column)
                    ).compareTo(groupName)!=0){ // Se o grupo da tupla carregada é diferente do grupo da tupla atual, ent�o sai do while true
                break;
            }
            // Processa todas as operações de agregação para cada operação
            agregationOperations.stream().forEach(agregationOperation -> agregationOperation.process(actualTuple,lastTupleLoaded));
            lastTupleLoaded = null;
        }

        // Aplica o finalize para todas as operações de agregação.
        if(actualTuple!=null)
            agregationOperations.stream().forEach(agregationOperation -> agregationOperation.finalize(actualTuple));
        return actualTuple;
    }

    @Override
    public Tuple next() {
        Tuple t= getNextTuple();
        actualTuple = null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return getNextTuple()!=null;
    }


    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String,List<String>> old = super.getContentInfo();
        Map<String,List<String>> map = new HashMap<>();
        map.put(source,new ArrayList<>());
        map.get(source).add(column);

        for (AgregationOperation ag:
             agregationOperations) {
            Map.Entry<String,String> name = ag.getNameDestination();
            if(map.get(name.getKey())==null){
                map.put(name.getKey(), new ArrayList<>());
            }
            map.get(name.getKey()).add(name.getValue());
        }

        return map;
    }
}
