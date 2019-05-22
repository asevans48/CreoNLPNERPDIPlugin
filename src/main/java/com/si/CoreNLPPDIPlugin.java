/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Describe your step plugin.
 * 
 */
public class CoreNLPPDIPlugin extends BaseStep implements StepInterface {
  private CoreNLPPDIPluginMeta meta;
  private CoreNLPPDIPluginData data;


  private static Class<?> PKG = CoreNLPPDIPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public CoreNLPPDIPlugin( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface
   *          The metadata to work with
   * @param stepDataInterface
   *          The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    this.data = (CoreNLPPDIPluginData) stepDataInterface;
    this.meta = (CoreNLPPDIPluginMeta) stepMetaInterface;
    return super.init( stepMetaInterface, stepDataInterface );
  }

  /**
   * Package an entity in a new row.
   *
   * @param rowMeta         The row meta
   * @param entity          The entity to add
   * @return                The updated row
   */
  private Object[] packageRow(RowMetaInterface rowMeta, String entity, Object[] r){
    int idx = rowMeta.indexOfValue(meta.getOutField());
    r[idx] = entity;
    return r;
  }

  /**
   * Get and set the address fields.
   *
   * @param rowMeta  The row meta interface
   * @param r        The existing row object without field values;
   * @return  The new row with values
   */
  private ArrayList<Object[]> computeRowValues(RowMetaInterface rowMeta, Object[] r){
    Object[] orow = r.clone();

    if(rowMeta.size() > r.length){
      orow = RowDataUtil.resizeArray(r, rowMeta.size());
    }

    ArrayList<Object[]> orows = new ArrayList<>();

    if(meta.getInField() != null) {
      int idx = rowMeta.indexOfValue(meta.getInField());
      String extractText = (String) r[idx];
      if(extractText != null){
        CoreDocument doc = new CoreDocument(extractText);
        data.getClassifier().annotate(doc);
        String entity = null;
        Stream<CoreLabel> stream = doc.tokens().stream();
        Iterator<CoreLabel> it = stream.iterator();
        while(it.hasNext()){
          CoreLabel token = it.next();
          if (token.ner().toUpperCase().trim().equals(meta.getEntityType())) {
            if(entity != null){
              entity = entity + " " + token.word();
            }else if(entity == null){
              entity = token.word();
            }
          }else if(entity != null){
            orows.add(packageRow(rowMeta, entity, r.clone()));
            entity = null;
          }
        }
        if(entity != null){
          orows.add(packageRow(rowMeta, entity, r.clone()));
        }
      }
    }
    return orows;
  }

  /**
   * Check if the value exists in the array
   *
   * @param arr  The array to check
   * @param v    The value in the array
   * @return  Whether the value exists
   */
  private int stringArrayContains(String[] arr, String v){
    int exists = -1;
    int i = 0;
    while(i < arr.length && exists == -1){
      if(arr[i].equals(v)){
        exists = i;
      }else {
        i += 1;
      }
    }
    return exists;
  }

  /**
   * Recreate the meta, adding the new fields.
   *
   * @param rowMeta   The row meta
   * @return  The changed row meta interface
   */
  private RowMetaInterface getNewRowMeta(RowMetaInterface rowMeta, CoreNLPPDIPluginMeta meta) throws KettleException {
    String[] fields = rowMeta.getFieldNames();
    String[] fieldnames = {meta.getOutField(), };

    int idx = stringArrayContains(fields, meta.getOutField());
    if(idx == -1){
      throw new KettleException("Libpostal Plugin missing output Field");
    }

    for(int i = 0; i < fieldnames.length; i++){
      String fname = fieldnames[i];
      int cidx = stringArrayContains(fields, fname);
      if(cidx == -1){
        ValueMetaInterface value = ValueMetaFactory.createValueMeta(fname, ValueMetaInterface.TYPE_STRING);
        rowMeta.addValueMeta(value);
      }
    }
    return rowMeta;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow();
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    if(first) {
      data.outputRowMeta = getInputRowMeta().clone();
      try {
        data.initClassifier(meta.getNerPath());
      }catch(IOException e){
        if(isBasic()){
          logBasic("Failed to Initialize NER");
          logBasic(meta.getNerPath());
        }
      }catch(ClassNotFoundException e){
        if(isBasic()){
          logBasic("Failed to Initialize NER");
          logBasic(meta.getNerPath());
        }
      }
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
      getNewRowMeta(data.outputRowMeta, meta);
      first = false;
    }

    ArrayList<Object[]> rows = computeRowValues(data.outputRowMeta, r);
    if(rows.size() > 0) {
      for (Object[] outRow : rows) {
        putRow(data.outputRowMeta, outRow); // copy row to possible alternate rowset(s).
      }
    }else{
      putRow(data.outputRowMeta, r); //should not reach, just in case
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "LibpostalExpanderPlugin.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }
}