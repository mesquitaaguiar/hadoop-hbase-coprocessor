package br.com.cameag.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Classe Coprocessor Hbase 
 * 
 * Evento avalia se o evento é Delete
 * Para cada deleção há a adição de linha na tabela final _INC
 * 
 * @author carlos@cameag.com.br
 * @since 0.1
 */
public class HBaseCoprocessor extends BaseRegionObserver {
	
	public static final Log LOG = LogFactory.getLog(HBaseCoprocessor.class);
	
	@Override
	public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
										MiniBatchOperationInProgress<Mutation> miniBatchOp){
		
		String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
		
		Configuration config = HBaseConfiguration.create();

		try (Connection conn = ConnectionFactory.createConnection(config);
				Table tblog = conn.getTable(TableName.valueOf(tableName + "_INC"));
					Table tbmain = conn.getTable(TableName.valueOf(tableName))) {
			
			for (int i = 0; i < miniBatchOp.size(); i++) { 
				
				Mutation operation = miniBatchOp.getOperation(i); 
				byte[] rkey = operation.getRow();
				
				NavigableMap<byte[], List<Cell>> familyCellMap = operation.getFamilyCellMap();
				
				Put p = new Put(rkey);
				
				for (Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) { 

					byte [] cfamily = CellUtil.cloneFamily(entry.getValue().iterator().next());
					Result closestRowBefore = c.getEnvironment().getRegion().getClosestRowBefore(rkey, cfamily);
					
					if (operation instanceof Delete){
						for (Cell c1 : closestRowBefore.rawCells()) {
							p.addColumn(CellUtil.cloneFamily(c1), CellUtil.cloneQualifier(c1), CellUtil.cloneValue(c1));
						}
						
						p.addColumn(Bytes.toBytes("control"), Bytes.toBytes("data"), 
								Bytes.toBytes(new SimpleDateFormat("yyyyymmddhhmmss").format(new Date())));
						tblog.put(p);
					}
					
				} 
			}

		} catch (IOException error) {
			error.printStackTrace();
		}
	}
}
