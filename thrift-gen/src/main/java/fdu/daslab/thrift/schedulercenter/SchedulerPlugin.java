/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package fdu.daslab.thrift.schedulercenter;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2021-05-24")
public class SchedulerPlugin {

  public interface Iface {

    public SchedulerResult schedule(java.util.List<fdu.daslab.thrift.base.Stage> stageList) throws org.apache.thrift.TException;

  }

  public interface AsyncIface {

    public void schedule(java.util.List<fdu.daslab.thrift.base.Stage> stageList, org.apache.thrift.async.AsyncMethodCallback<SchedulerResult> resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends org.apache.thrift.TServiceClient implements Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      super(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
      super(iprot, oprot);
    }

    public SchedulerResult schedule(java.util.List<fdu.daslab.thrift.base.Stage> stageList) throws org.apache.thrift.TException
    {
      send_schedule(stageList);
      return recv_schedule();
    }

    public void send_schedule(java.util.List<fdu.daslab.thrift.base.Stage> stageList) throws org.apache.thrift.TException
    {
      schedule_args args = new schedule_args();
      args.setStageList(stageList);
      sendBase("schedule", args);
    }

    public SchedulerResult recv_schedule() throws org.apache.thrift.TException
    {
      schedule_result result = new schedule_result();
      receiveBase(result, "schedule");
      if (result.isSetSuccess()) {
        return result.success;
      }
      throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "schedule failed: unknown result");
    }

  }
  public static class AsyncClient extends org.apache.thrift.async.TAsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void schedule(java.util.List<fdu.daslab.thrift.base.Stage> stageList, org.apache.thrift.async.AsyncMethodCallback<SchedulerResult> resultHandler) throws org.apache.thrift.TException {
      checkReady();
      schedule_call method_call = new schedule_call(stageList, resultHandler, this, ___protocolFactory, ___transport);
      this.___currentMethod = method_call;
      ___manager.call(method_call);
    }

    public static class schedule_call extends org.apache.thrift.async.TAsyncMethodCall<SchedulerResult> {
      private java.util.List<fdu.daslab.thrift.base.Stage> stageList;
      public schedule_call(java.util.List<fdu.daslab.thrift.base.Stage> stageList, org.apache.thrift.async.AsyncMethodCallback<SchedulerResult> resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.stageList = stageList;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("schedule", org.apache.thrift.protocol.TMessageType.CALL, 0));
        schedule_args args = new schedule_args();
        args.setStageList(stageList);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public SchedulerResult getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new java.lang.IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_schedule();
      }
    }

  }

  public static class Processor<I extends Iface> extends org.apache.thrift.TBaseProcessor<I> implements org.apache.thrift.TProcessor {
    private static final org.slf4j.Logger _LOGGER = org.slf4j.LoggerFactory.getLogger(Processor.class.getName());
    public Processor(I iface) {
      super(iface, getProcessMap(new java.util.HashMap<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>>()));
    }

    protected Processor(I iface, java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends Iface> java.util.Map<java.lang.String,  org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> getProcessMap(java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      processMap.put("schedule", new schedule());
      return processMap;
    }

    public static class schedule<I extends Iface> extends org.apache.thrift.ProcessFunction<I, schedule_args> {
      public schedule() {
        super("schedule");
      }

      public schedule_args getEmptyArgsInstance() {
        return new schedule_args();
      }

      protected boolean isOneway() {
        return false;
      }

      @Override
      protected boolean rethrowUnhandledExceptions() {
        return false;
      }

      public schedule_result getResult(I iface, schedule_args args) throws org.apache.thrift.TException {
        schedule_result result = new schedule_result();
        result.success = iface.schedule(args.stageList);
        return result;
      }
    }

  }

  public static class AsyncProcessor<I extends AsyncIface> extends org.apache.thrift.TBaseAsyncProcessor<I> {
    private static final org.slf4j.Logger _LOGGER = org.slf4j.LoggerFactory.getLogger(AsyncProcessor.class.getName());
    public AsyncProcessor(I iface) {
      super(iface, getProcessMap(new java.util.HashMap<java.lang.String, org.apache.thrift.AsyncProcessFunction<I, ? extends org.apache.thrift.TBase, ?>>()));
    }

    protected AsyncProcessor(I iface, java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends AsyncIface> java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase,?>> getProcessMap(java.util.Map<java.lang.String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      processMap.put("schedule", new schedule());
      return processMap;
    }

    public static class schedule<I extends AsyncIface> extends org.apache.thrift.AsyncProcessFunction<I, schedule_args, SchedulerResult> {
      public schedule() {
        super("schedule");
      }

      public schedule_args getEmptyArgsInstance() {
        return new schedule_args();
      }

      public org.apache.thrift.async.AsyncMethodCallback<SchedulerResult> getResultHandler(final org.apache.thrift.server.AbstractNonblockingServer.AsyncFrameBuffer fb, final int seqid) {
        final org.apache.thrift.AsyncProcessFunction fcall = this;
        return new org.apache.thrift.async.AsyncMethodCallback<SchedulerResult>() { 
          public void onComplete(SchedulerResult o) {
            schedule_result result = new schedule_result();
            result.success = o;
            try {
              fcall.sendResponse(fb, result, org.apache.thrift.protocol.TMessageType.REPLY,seqid);
            } catch (org.apache.thrift.transport.TTransportException e) {
              _LOGGER.error("TTransportException writing to internal frame buffer", e);
              fb.close();
            } catch (java.lang.Exception e) {
              _LOGGER.error("Exception writing to internal frame buffer", e);
              onError(e);
            }
          }
          public void onError(java.lang.Exception e) {
            byte msgType = org.apache.thrift.protocol.TMessageType.REPLY;
            org.apache.thrift.TSerializable msg;
            schedule_result result = new schedule_result();
            if (e instanceof org.apache.thrift.transport.TTransportException) {
              _LOGGER.error("TTransportException inside handler", e);
              fb.close();
              return;
            } else if (e instanceof org.apache.thrift.TApplicationException) {
              _LOGGER.error("TApplicationException inside handler", e);
              msgType = org.apache.thrift.protocol.TMessageType.EXCEPTION;
              msg = (org.apache.thrift.TApplicationException)e;
            } else {
              _LOGGER.error("Exception inside handler", e);
              msgType = org.apache.thrift.protocol.TMessageType.EXCEPTION;
              msg = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.INTERNAL_ERROR, e.getMessage());
            }
            try {
              fcall.sendResponse(fb,msg,msgType,seqid);
            } catch (java.lang.Exception ex) {
              _LOGGER.error("Exception writing to internal frame buffer", ex);
              fb.close();
            }
          }
        };
      }

      protected boolean isOneway() {
        return false;
      }

      public void start(I iface, schedule_args args, org.apache.thrift.async.AsyncMethodCallback<SchedulerResult> resultHandler) throws org.apache.thrift.TException {
        iface.schedule(args.stageList,resultHandler);
      }
    }

  }

  public static class schedule_args implements org.apache.thrift.TBase<schedule_args, schedule_args._Fields>, java.io.Serializable, Cloneable, Comparable<schedule_args>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("schedule_args");

    private static final org.apache.thrift.protocol.TField STAGE_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("stageList", org.apache.thrift.protocol.TType.LIST, (short)1);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new schedule_argsStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new schedule_argsTupleSchemeFactory();

    public @org.apache.thrift.annotation.Nullable java.util.List<fdu.daslab.thrift.base.Stage> stageList; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      STAGE_LIST((short)1, "stageList");

      private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

      static {
        for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // STAGE_LIST
            return STAGE_LIST;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByName(java.lang.String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final java.lang.String _fieldName;

      _Fields(short thriftId, java.lang.String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public java.lang.String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.STAGE_LIST, new org.apache.thrift.meta_data.FieldMetaData("stageList", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
              new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, fdu.daslab.thrift.base.Stage.class))));
      metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(schedule_args.class, metaDataMap);
    }

    public schedule_args() {
    }

    public schedule_args(
      java.util.List<fdu.daslab.thrift.base.Stage> stageList)
    {
      this();
      this.stageList = stageList;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public schedule_args(schedule_args other) {
      if (other.isSetStageList()) {
        java.util.List<fdu.daslab.thrift.base.Stage> __this__stageList = new java.util.ArrayList<fdu.daslab.thrift.base.Stage>(other.stageList.size());
        for (fdu.daslab.thrift.base.Stage other_element : other.stageList) {
          __this__stageList.add(new fdu.daslab.thrift.base.Stage(other_element));
        }
        this.stageList = __this__stageList;
      }
    }

    public schedule_args deepCopy() {
      return new schedule_args(this);
    }

    @Override
    public void clear() {
      this.stageList = null;
    }

    public int getStageListSize() {
      return (this.stageList == null) ? 0 : this.stageList.size();
    }

    @org.apache.thrift.annotation.Nullable
    public java.util.Iterator<fdu.daslab.thrift.base.Stage> getStageListIterator() {
      return (this.stageList == null) ? null : this.stageList.iterator();
    }

    public void addToStageList(fdu.daslab.thrift.base.Stage elem) {
      if (this.stageList == null) {
        this.stageList = new java.util.ArrayList<fdu.daslab.thrift.base.Stage>();
      }
      this.stageList.add(elem);
    }

    @org.apache.thrift.annotation.Nullable
    public java.util.List<fdu.daslab.thrift.base.Stage> getStageList() {
      return this.stageList;
    }

    public schedule_args setStageList(@org.apache.thrift.annotation.Nullable java.util.List<fdu.daslab.thrift.base.Stage> stageList) {
      this.stageList = stageList;
      return this;
    }

    public void unsetStageList() {
      this.stageList = null;
    }

    /** Returns true if field stageList is set (has been assigned a value) and false otherwise */
    public boolean isSetStageList() {
      return this.stageList != null;
    }

    public void setStageListIsSet(boolean value) {
      if (!value) {
        this.stageList = null;
      }
    }

    public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
      switch (field) {
      case STAGE_LIST:
        if (value == null) {
          unsetStageList();
        } else {
          setStageList((java.util.List<fdu.daslab.thrift.base.Stage>)value);
        }
        break;

      }
    }

    @org.apache.thrift.annotation.Nullable
    public java.lang.Object getFieldValue(_Fields field) {
      switch (field) {
      case STAGE_LIST:
        return getStageList();

      }
      throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new java.lang.IllegalArgumentException();
      }

      switch (field) {
      case STAGE_LIST:
        return isSetStageList();
      }
      throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
      if (that == null)
        return false;
      if (that instanceof schedule_args)
        return this.equals((schedule_args)that);
      return false;
    }

    public boolean equals(schedule_args that) {
      if (that == null)
        return false;
      if (this == that)
        return true;

      boolean this_present_stageList = true && this.isSetStageList();
      boolean that_present_stageList = true && that.isSetStageList();
      if (this_present_stageList || that_present_stageList) {
        if (!(this_present_stageList && that_present_stageList))
          return false;
        if (!this.stageList.equals(that.stageList))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;

      hashCode = hashCode * 8191 + ((isSetStageList()) ? 131071 : 524287);
      if (isSetStageList())
        hashCode = hashCode * 8191 + stageList.hashCode();

      return hashCode;
    }

    @Override
    public int compareTo(schedule_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = java.lang.Boolean.valueOf(isSetStageList()).compareTo(other.isSetStageList());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetStageList()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stageList, other.stageList);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    @org.apache.thrift.annotation.Nullable
    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      scheme(iprot).read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString() {
      java.lang.StringBuilder sb = new java.lang.StringBuilder("schedule_args(");
      boolean first = true;

      sb.append("stageList:");
      if (this.stageList == null) {
        sb.append("null");
      } else {
        sb.append(this.stageList);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class schedule_argsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      public schedule_argsStandardScheme getScheme() {
        return new schedule_argsStandardScheme();
      }
    }

    private static class schedule_argsStandardScheme extends org.apache.thrift.scheme.StandardScheme<schedule_args> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, schedule_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 1: // STAGE_LIST
              if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
                {
                  org.apache.thrift.protocol.TList _list38 = iprot.readListBegin();
                  struct.stageList = new java.util.ArrayList<fdu.daslab.thrift.base.Stage>(_list38.size);
                  @org.apache.thrift.annotation.Nullable fdu.daslab.thrift.base.Stage _elem39;
                  for (int _i40 = 0; _i40 < _list38.size; ++_i40)
                  {
                    _elem39 = new fdu.daslab.thrift.base.Stage();
                    _elem39.read(iprot);
                    struct.stageList.add(_elem39);
                  }
                  iprot.readListEnd();
                }
                struct.setStageListIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, schedule_args struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.stageList != null) {
          oprot.writeFieldBegin(STAGE_LIST_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.stageList.size()));
            for (fdu.daslab.thrift.base.Stage _iter41 : struct.stageList)
            {
              _iter41.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class schedule_argsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      public schedule_argsTupleScheme getScheme() {
        return new schedule_argsTupleScheme();
      }
    }

    private static class schedule_argsTupleScheme extends org.apache.thrift.scheme.TupleScheme<schedule_args> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, schedule_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
        java.util.BitSet optionals = new java.util.BitSet();
        if (struct.isSetStageList()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.isSetStageList()) {
          {
            oprot.writeI32(struct.stageList.size());
            for (fdu.daslab.thrift.base.Stage _iter42 : struct.stageList)
            {
              _iter42.write(oprot);
            }
          }
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, schedule_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
        java.util.BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          {
            org.apache.thrift.protocol.TList _list43 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
            struct.stageList = new java.util.ArrayList<fdu.daslab.thrift.base.Stage>(_list43.size);
            @org.apache.thrift.annotation.Nullable fdu.daslab.thrift.base.Stage _elem44;
            for (int _i45 = 0; _i45 < _list43.size; ++_i45)
            {
              _elem44 = new fdu.daslab.thrift.base.Stage();
              _elem44.read(iprot);
              struct.stageList.add(_elem44);
            }
          }
          struct.setStageListIsSet(true);
        }
      }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
      return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
    }
  }

  public static class schedule_result implements org.apache.thrift.TBase<schedule_result, schedule_result._Fields>, java.io.Serializable, Cloneable, Comparable<schedule_result>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("schedule_result");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.STRUCT, (short)0);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new schedule_resultStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new schedule_resultTupleSchemeFactory();

    public @org.apache.thrift.annotation.Nullable SchedulerResult success; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      SUCCESS((short)0, "success");

      private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

      static {
        for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      @org.apache.thrift.annotation.Nullable
      public static _Fields findByName(java.lang.String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final java.lang.String _fieldName;

      _Fields(short thriftId, java.lang.String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public java.lang.String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SchedulerResult.class)));
      metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(schedule_result.class, metaDataMap);
    }

    public schedule_result() {
    }

    public schedule_result(
      SchedulerResult success)
    {
      this();
      this.success = success;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public schedule_result(schedule_result other) {
      if (other.isSetSuccess()) {
        this.success = new SchedulerResult(other.success);
      }
    }

    public schedule_result deepCopy() {
      return new schedule_result(this);
    }

    @Override
    public void clear() {
      this.success = null;
    }

    @org.apache.thrift.annotation.Nullable
    public SchedulerResult getSuccess() {
      return this.success;
    }

    public schedule_result setSuccess(@org.apache.thrift.annotation.Nullable SchedulerResult success) {
      this.success = success;
      return this;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((SchedulerResult)value);
        }
        break;

      }
    }

    @org.apache.thrift.annotation.Nullable
    public java.lang.Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return getSuccess();

      }
      throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new java.lang.IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return isSetSuccess();
      }
      throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
      if (that == null)
        return false;
      if (that instanceof schedule_result)
        return this.equals((schedule_result)that);
      return false;
    }

    public boolean equals(schedule_result that) {
      if (that == null)
        return false;
      if (this == that)
        return true;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;

      hashCode = hashCode * 8191 + ((isSetSuccess()) ? 131071 : 524287);
      if (isSetSuccess())
        hashCode = hashCode * 8191 + success.hashCode();

      return hashCode;
    }

    @Override
    public int compareTo(schedule_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = java.lang.Boolean.valueOf(isSetSuccess()).compareTo(other.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, other.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    @org.apache.thrift.annotation.Nullable
    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      scheme(iprot).read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      scheme(oprot).write(oprot, this);
      }

    @Override
    public java.lang.String toString() {
      java.lang.StringBuilder sb = new java.lang.StringBuilder("schedule_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
      if (success != null) {
        success.validate();
      }
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class schedule_resultStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      public schedule_resultStandardScheme getScheme() {
        return new schedule_resultStandardScheme();
      }
    }

    private static class schedule_resultStandardScheme extends org.apache.thrift.scheme.StandardScheme<schedule_result> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, schedule_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 0: // SUCCESS
              if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
                struct.success = new SchedulerResult();
                struct.success.read(iprot);
                struct.setSuccessIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, schedule_result struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.success != null) {
          oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
          struct.success.write(oprot);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class schedule_resultTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
      public schedule_resultTupleScheme getScheme() {
        return new schedule_resultTupleScheme();
      }
    }

    private static class schedule_resultTupleScheme extends org.apache.thrift.scheme.TupleScheme<schedule_result> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, schedule_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
        java.util.BitSet optionals = new java.util.BitSet();
        if (struct.isSetSuccess()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.isSetSuccess()) {
          struct.success.write(oprot);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, schedule_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
        java.util.BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          struct.success = new SchedulerResult();
          struct.success.read(iprot);
          struct.setSuccessIsSet(true);
        }
      }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
      return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
    }
  }

}