// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/protobuf/struct.proto

package org.apache.hadoop.hbase.shaded.com.google.protobuf;

/**
 * <pre>
 * `ListValue` is a wrapper around a repeated field of values.
 * The JSON representation for `ListValue` is JSON array.
 * </pre>
 *
 * Protobuf type {@code google.protobuf.ListValue}
 */
public  final class ListValue extends
    org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:google.protobuf.ListValue)
    ListValueOrBuilder {
  // Use ListValue.newBuilder() to construct.
  private ListValue(org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ListValue() {
    values_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private ListValue(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
              values_ = new java.util.ArrayList<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value>();
              mutable_bitField0_ |= 0x00000001;
            }
            values_.add(
                input.readMessage(org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.parser(), extensionRegistry));
            break;
          }
        }
      }
    } catch (org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
        values_ = java.util.Collections.unmodifiableList(values_);
      }
      makeExtensionsImmutable();
    }
  }
  public static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.StructProto.internal_static_google_protobuf_ListValue_descriptor;
  }

  protected org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.StructProto.internal_static_google_protobuf_ListValue_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.class, org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.Builder.class);
  }

  public static final int VALUES_FIELD_NUMBER = 1;
  private java.util.List<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value> values_;
  /**
   * <pre>
   * Repeated field of dynamically typed values.
   * </pre>
   *
   * <code>repeated .google.protobuf.Value values = 1;</code>
   */
  public java.util.List<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value> getValuesList() {
    return values_;
  }
  /**
   * <pre>
   * Repeated field of dynamically typed values.
   * </pre>
   *
   * <code>repeated .google.protobuf.Value values = 1;</code>
   */
  public java.util.List<? extends org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder> 
      getValuesOrBuilderList() {
    return values_;
  }
  /**
   * <pre>
   * Repeated field of dynamically typed values.
   * </pre>
   *
   * <code>repeated .google.protobuf.Value values = 1;</code>
   */
  public int getValuesCount() {
    return values_.size();
  }
  /**
   * <pre>
   * Repeated field of dynamically typed values.
   * </pre>
   *
   * <code>repeated .google.protobuf.Value values = 1;</code>
   */
  public org.apache.hadoop.hbase.shaded.com.google.protobuf.Value getValues(int index) {
    return values_.get(index);
  }
  /**
   * <pre>
   * Repeated field of dynamically typed values.
   * </pre>
   *
   * <code>repeated .google.protobuf.Value values = 1;</code>
   */
  public org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder getValuesOrBuilder(
      int index) {
    return values_.get(index);
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    for (int i = 0; i < values_.size(); i++) {
      output.writeMessage(1, values_.get(i));
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < values_.size(); i++) {
      size += org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, values_.get(i));
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue)) {
      return super.equals(obj);
    }
    org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue other = (org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue) obj;

    boolean result = true;
    result = result && getValuesList()
        .equals(other.getValuesList());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    if (getValuesCount() > 0) {
      hash = (37 * hash) + VALUES_FIELD_NUMBER;
      hash = (53 * hash) + getValuesList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString data)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString data,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(byte[] data)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      byte[] data,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      java.io.InputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseDelimitedFrom(
      java.io.InputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * <pre>
   * `ListValue` is a wrapper around a repeated field of values.
   * The JSON representation for `ListValue` is JSON array.
   * </pre>
   *
   * Protobuf type {@code google.protobuf.ListValue}
   */
  public static final class Builder extends
      org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.protobuf.ListValue)
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValueOrBuilder {
    public static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.StructProto.internal_static_google_protobuf_ListValue_descriptor;
    }

    protected org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.StructProto.internal_static_google_protobuf_ListValue_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.class, org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.Builder.class);
    }

    // Construct using org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getValuesFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (valuesBuilder_ == null) {
        values_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        valuesBuilder_.clear();
      }
      return this;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.StructProto.internal_static_google_protobuf_ListValue_descriptor;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue getDefaultInstanceForType() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.getDefaultInstance();
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue build() {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue buildPartial() {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue result = new org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue(this);
      int from_bitField0_ = bitField0_;
      if (valuesBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          values_ = java.util.Collections.unmodifiableList(values_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.values_ = values_;
      } else {
        result.values_ = valuesBuilder_.build();
      }
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(org.apache.hadoop.hbase.shaded.com.google.protobuf.Message other) {
      if (other instanceof org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue) {
        return mergeFrom((org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue other) {
      if (other == org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue.getDefaultInstance()) return this;
      if (valuesBuilder_ == null) {
        if (!other.values_.isEmpty()) {
          if (values_.isEmpty()) {
            values_ = other.values_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureValuesIsMutable();
            values_.addAll(other.values_);
          }
          onChanged();
        }
      } else {
        if (!other.values_.isEmpty()) {
          if (valuesBuilder_.isEmpty()) {
            valuesBuilder_.dispose();
            valuesBuilder_ = null;
            values_ = other.values_;
            bitField0_ = (bitField0_ & ~0x00000001);
            valuesBuilder_ = 
              org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getValuesFieldBuilder() : null;
          } else {
            valuesBuilder_.addAllMessages(other.values_);
          }
        }
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input,
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value> values_ =
      java.util.Collections.emptyList();
    private void ensureValuesIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        values_ = new java.util.ArrayList<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value>(values_);
        bitField0_ |= 0x00000001;
       }
    }

    private org.apache.hadoop.hbase.shaded.com.google.protobuf.RepeatedFieldBuilderV3<
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Value, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder, org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder> valuesBuilder_;

    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public java.util.List<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value> getValuesList() {
      if (valuesBuilder_ == null) {
        return java.util.Collections.unmodifiableList(values_);
      } else {
        return valuesBuilder_.getMessageList();
      }
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public int getValuesCount() {
      if (valuesBuilder_ == null) {
        return values_.size();
      } else {
        return valuesBuilder_.getCount();
      }
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Value getValues(int index) {
      if (valuesBuilder_ == null) {
        return values_.get(index);
      } else {
        return valuesBuilder_.getMessage(index);
      }
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder setValues(
        int index, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value value) {
      if (valuesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureValuesIsMutable();
        values_.set(index, value);
        onChanged();
      } else {
        valuesBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder setValues(
        int index, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder builderForValue) {
      if (valuesBuilder_ == null) {
        ensureValuesIsMutable();
        values_.set(index, builderForValue.build());
        onChanged();
      } else {
        valuesBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder addValues(org.apache.hadoop.hbase.shaded.com.google.protobuf.Value value) {
      if (valuesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureValuesIsMutable();
        values_.add(value);
        onChanged();
      } else {
        valuesBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder addValues(
        int index, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value value) {
      if (valuesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureValuesIsMutable();
        values_.add(index, value);
        onChanged();
      } else {
        valuesBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder addValues(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder builderForValue) {
      if (valuesBuilder_ == null) {
        ensureValuesIsMutable();
        values_.add(builderForValue.build());
        onChanged();
      } else {
        valuesBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder addValues(
        int index, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder builderForValue) {
      if (valuesBuilder_ == null) {
        ensureValuesIsMutable();
        values_.add(index, builderForValue.build());
        onChanged();
      } else {
        valuesBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder addAllValues(
        java.lang.Iterable<? extends org.apache.hadoop.hbase.shaded.com.google.protobuf.Value> values) {
      if (valuesBuilder_ == null) {
        ensureValuesIsMutable();
        org.apache.hadoop.hbase.shaded.com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, values_);
        onChanged();
      } else {
        valuesBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder clearValues() {
      if (valuesBuilder_ == null) {
        values_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        valuesBuilder_.clear();
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public Builder removeValues(int index) {
      if (valuesBuilder_ == null) {
        ensureValuesIsMutable();
        values_.remove(index);
        onChanged();
      } else {
        valuesBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder getValuesBuilder(
        int index) {
      return getValuesFieldBuilder().getBuilder(index);
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder getValuesOrBuilder(
        int index) {
      if (valuesBuilder_ == null) {
        return values_.get(index);  } else {
        return valuesBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public java.util.List<? extends org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder> 
         getValuesOrBuilderList() {
      if (valuesBuilder_ != null) {
        return valuesBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(values_);
      }
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder addValuesBuilder() {
      return getValuesFieldBuilder().addBuilder(
          org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.getDefaultInstance());
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder addValuesBuilder(
        int index) {
      return getValuesFieldBuilder().addBuilder(
          index, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.getDefaultInstance());
    }
    /**
     * <pre>
     * Repeated field of dynamically typed values.
     * </pre>
     *
     * <code>repeated .google.protobuf.Value values = 1;</code>
     */
    public java.util.List<org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder> 
         getValuesBuilderList() {
      return getValuesFieldBuilder().getBuilderList();
    }
    private org.apache.hadoop.hbase.shaded.com.google.protobuf.RepeatedFieldBuilderV3<
        org.apache.hadoop.hbase.shaded.com.google.protobuf.Value, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder, org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder> 
        getValuesFieldBuilder() {
      if (valuesBuilder_ == null) {
        valuesBuilder_ = new org.apache.hadoop.hbase.shaded.com.google.protobuf.RepeatedFieldBuilderV3<
            org.apache.hadoop.hbase.shaded.com.google.protobuf.Value, org.apache.hadoop.hbase.shaded.com.google.protobuf.Value.Builder, org.apache.hadoop.hbase.shaded.com.google.protobuf.ValueOrBuilder>(
                values_,
                ((bitField0_ & 0x00000001) == 0x00000001),
                getParentForChildren(),
                isClean());
        values_ = null;
      }
      return valuesBuilder_;
    }
    public final Builder setUnknownFields(
        final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:google.protobuf.ListValue)
  }

  // @@protoc_insertion_point(class_scope:google.protobuf.ListValue)
  private static final org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue();
  }

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<ListValue>
      PARSER = new org.apache.hadoop.hbase.shaded.com.google.protobuf.AbstractParser<ListValue>() {
    public ListValue parsePartialFrom(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input,
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
        return new ListValue(input, extensionRegistry);
    }
  };

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<ListValue> parser() {
    return PARSER;
  }

  @java.lang.Override
  public org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<ListValue> getParserForType() {
    return PARSER;
  }

  public org.apache.hadoop.hbase.shaded.com.google.protobuf.ListValue getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

