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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class CoreNLPPDIPluginDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = CoreNLPPDIPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;

  private static final int LARGE_FIELD = 350;
  private static final int MEDIUM_FIELD = 250;
  private static final int SMALL_FIELD = 75;

  private CoreNLPPDIPluginMeta meta;

  private Label wlStepname;
  private Text wStepname;
  private FormData fdStepname, fdlStepname;

  private Label lfname;
  private CCombo wInFieldCombo;
  private FormData fdlFname, fdStep;

  private Label wOutFieldName;
  private TextVar wOutField;
  private FormData fdlOutFieldName, fdlOutField;

  private Label wEntityName;
  private CCombo wEntityCombo;
  private FormData fdlEntityName, fdlEntityOut;

  private Label wNerName;
  private TextVar wNerOut;
  private FormData fdlNerName, fdlNerOut;

  private Button wCancel;
  private Button wOK;
  private ModifyListener lsMod;
  private Listener lsCancel;
  private Listener lsOK;
  private SelectionAdapter lsDef;
  private boolean changed;

  public CoreNLPPDIPluginDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (CoreNLPPDIPluginMeta) in;
  }

  public String open() {
    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseStepDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        meta.setChanged();
      }
    };

    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "CoreNLPPDIPluginDialog.Shell.Title"));
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG, "CoreNLPPDIPluginDialog.Stepname.Label"));
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);

    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);

    // Set the in field name
    lfname = new Label( shell, SWT.RIGHT );
    lfname.setText( BaseMessages.getString( PKG, "CoreNLPPDIPluginDialog.Fields.FieldName" ) );
    props.setLook( lfname );
    fdlFname = new FormData();
    fdlFname.left = new FormAttachment( 0, 0 );
    fdlFname.right = new FormAttachment( middle, -margin );
    fdlFname.top = new FormAttachment( wlStepname, 15 );
    lfname.setLayoutData( fdlFname );

    wInFieldCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wInFieldCombo );
    StepMeta stepinfo = transMeta.findStep( stepname );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wInFieldCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wInFieldCombo.addModifyListener( lsMod );
    fdStep = new FormData();
    fdStep.left = new FormAttachment( middle, 0 );
    fdStep.top = new FormAttachment( wlStepname, 15 );
    fdStep.right = new FormAttachment( 100, 0 );
    wInFieldCombo.setLayoutData( fdStep );

    // set the out field name
    wOutFieldName = new Label( shell, SWT.RIGHT );
    wOutFieldName.setText( BaseMessages.getString( PKG, "CoreNLPPDIPluginDialog.Fields.OutFieldName" ) );
    props.setLook( wOutFieldName );
    fdlOutFieldName = new FormData();
    fdlOutFieldName.left = new FormAttachment( 0, 0 );
    fdlOutFieldName.right = new FormAttachment( middle, -margin );
    fdlOutFieldName.top = new FormAttachment( lfname, 15 );
    wOutFieldName.setLayoutData( fdlOutFieldName );

    wOutField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutField.setText("");
    wOutField.addModifyListener(lsMod);
    props.setLook(wOutField);
    fdlOutField = new FormData();
    fdlOutField.left = new FormAttachment(middle, 0);
    fdlOutField.top = new FormAttachment(lfname, 15);
    fdlOutField.right = new FormAttachment(100, 0);
    wOutField.setLayoutData(fdlOutField);

    // set the entity name
    wEntityName = new Label( shell, SWT.RIGHT );
    wEntityName.setText( BaseMessages.getString( PKG, "CoreNLPPDIPluginDialog.Fields.EntityType" ) );
    props.setLook( wEntityName );
    fdlEntityName = new FormData();
    fdlEntityName.left = new FormAttachment( 0, 0 );
    fdlEntityName.right = new FormAttachment( middle, -margin );
    fdlEntityName.top = new FormAttachment( wOutField, 15 );
    wEntityName.setLayoutData( fdlEntityName );

    wEntityCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wEntityCombo );
    if ( stepinfo != null ) {
      String[] fields = {"PERSON", "ORGANIZATION", "LOCATION"};
      for (int i = 0; i < fields.length; i++) {
        wEntityCombo.add(fields[i]);
      }
    }

    wEntityCombo.addModifyListener( lsMod );
    fdlEntityOut = new FormData();
    fdlEntityOut.left = new FormAttachment( middle, 0 );
    fdlEntityOut.top = new FormAttachment( wOutField, 15 );
    fdlEntityOut.right = new FormAttachment( 100, 0 );
    wEntityCombo.setLayoutData( fdlEntityOut );

    // set the NER path
    wNerName = new Label(shell, SWT.RIGHT);
    wNerName.setText(BaseMessages.getString(PKG, "CoreNLPPDIPluginDialog.Fields.NerPath"));
    props.setLook(wNerName);
    fdlNerName = new FormData();
    fdlNerName.left = new FormAttachment(0, 0);
    fdlNerName.top = new FormAttachment(wEntityName, 15);
    fdlNerName.right = new FormAttachment(middle, -margin);
    wNerName.setLayoutData(fdlNerName);

    wNerOut = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNerOut.setText("");
    wNerOut.addModifyListener(lsMod);
    props.setLook(wNerOut);
    fdlNerOut = new FormData();
    fdlNerOut.left = new FormAttachment(middle, 0);
    fdlNerOut.top = new FormAttachment(wEntityName, 15);
    fdlNerOut.right = new FormAttachment(100, 0);
    wNerOut.setLayoutData(fdlNerOut);

    // OK and cancel buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[]{wOK, wCancel}, margin, wNerName);

    // Add listeners for cancel and OK
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };
    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    // default listener (for hitting "enter")
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wStepname.addSelectionListener(lsDef);
    wEntityCombo.addSelectionListener(lsDef);
    wOutField.addSelectionListener(lsDef);
    wNerOut.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseStepDialog
    setSize();

    // populate the dialog with the values from the meta object
    getData();

    // restore the changed flag to original value, as the modify listeners fire during dialog population
    meta.setChanged(changed);

    // open dialog and enter event loop
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "stepname" variable is inherited from BaseStepDialog
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wStepname.selectAll();
    wEntityCombo.setText(Const.NVL(meta.getEntityType(), "LOCATION"));
    wInFieldCombo.setText(Const.NVL(meta.getInField(), ""));
    wNerOut.setText(Const.NVL(meta.getNerPath(), ""));
    wOutField.setText(Const.NVL(meta.getOutField(), ""));
    wStepname.setFocus();
  }

  private Image getImage() {
    PluginInterface plugin =
            PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[0];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
              ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  /**
   * Called when the user cancels the dialog.
   */
  private void cancel() {
    // The "stepname" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    stepname = null;
    // Restoring original "changed" flag on the met aobject
    meta.setChanged( changed );
    // close the SWT dialog window
    dispose();
  }


  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    meta.setEntityType(wEntityCombo.getText());
    meta.setInField(wInFieldCombo.getText());
    meta.setNerPath(wNerOut.getText());
    meta.setOutField(wOutField.getText());
    dispose();
  }
}