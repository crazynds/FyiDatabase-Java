package sgbd.query.unaryop;

import engine.util.Util;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.agregation.AgregationOperation;


import java.util.*;

public class GroupOperator extends SimpleUnaryOperator {

    private String source,column;

    private BigKey groupName;
    private Tuple lastTupleLoaded;
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
        groupName = null;
    }

    @Override
    public Tuple getNextTuple(){
        Tuple actualTuple = null;
        while(operator.hasNext()){
            // Se não tem a prox tupla carregada, carrega ela
            if(lastTupleLoaded==null)
                lastTupleLoaded = operator.next();


            // Se a tupla de acumulador n existir, cria ela e já vincula a agregação atual
            if(actualTuple ==null){
                // Cria a tupla
                actualTuple = new Tuple();
                RowData rowGroup = new RowData();
                rowGroup.setField(column,lastTupleLoaded.getContent(source).getField(column),lastTupleLoaded.getContent(source).getMetadata(column));
                // Seta nela o dado inicial
                actualTuple.setContent(source,rowGroup);
                groupName = new BigKey(lastTupleLoaded.getContent(source).getData(column));
                // Chama a inicialização de todas as operações de agregações
                final Tuple t = actualTuple;
                agregationOperations.stream().forEach(agregationOperation -> agregationOperation.initialize(t));
            }else if(new BigKey(lastTupleLoaded.getContent(source).getData(column)).compareTo(groupName)!=0){ // Se o grupo da tupla carregada é diferente do grupo da tupla atual, então sai do while true
                break;
            }
            // Processa todas as operações de agregação para cada operação
            final Tuple t = actualTuple;
            agregationOperations.stream().forEach(agregationOperation -> agregationOperation.process(t,lastTupleLoaded));
            lastTupleLoaded = null;
        }

        // Aplica o finalize para todas as operações de agregação.
        if(actualTuple!=null) {
            final Tuple t = actualTuple;
            agregationOperations.stream().forEach(agregationOperation -> agregationOperation.finalize(t));
        }
        return actualTuple;
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String,List<String>> old = super.getContentInfo();
        Map<String,List<String>> map = new LinkedHashMap<>();
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
