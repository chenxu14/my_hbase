// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/protobuf/wrappers.proto

package org.apache.hadoop.hbase.shaded.com.google.protobuf;

/**
 * <pre>
 * Wrapper message for `string`.
 * The JSON representation for `StringValue` is JSON string.
 * </pre>
 *
 * Protobuf type {@code google.protobuf.StringValue}
 */
public  final class StringValue extends
    org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:google.protobuf.StringValue)
    StringValueOrBuilder {
  // Use StringValue.newBuilder() to construct.
  private StringValue(org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private StringValue() {
    value_ = "";
  }

  @java.lang.Override
  public final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private StringValue(
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
            java.lang.String s = input.readStringRequireUtf8();

            value_ = s;
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
      makeExtensionsImmutable();
    }
  }
  public static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.WrappersProto.internal_static_google_protobuf_StringValue_descriptor;
  }

  protected org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.WrappersProto.internal_static_google_protobuf_StringValue_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.class, org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.Builder.class);
  }

  public static final int VALUE_FIELD_NUMBER = 1;
  private volatile java.lang.Object value_;
  /**
   * <pre>
   * The string value.
   * </pre>
   *
   * <code>optional string value = 1;</code>
   */
  public java.lang.String getValue() {
    java.lang.Object ref = value_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString bs = 
          (org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      value_ = s;
      return s;
    }
  }
  /**
   * <pre>
   * The string value.
   * </pre>
   *
   * <code>optional string value = 1;</code>
   */
  public org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString
      getValueBytes() {
    java.lang.Object ref = value_;
    if (ref instanceof java.lang.String) {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString b = 
          org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      value_ = b;
      return b;
    } else {
      return (org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString) ref;
    }
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
    if (!getValueBytes().isEmpty()) {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.writeString(output, 1, value_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getValueBytes().isEmpty()) {
      size += org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.computeStringSize(1, value_);
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
    if (!(obj instanceof org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue)) {
      return super.equals(obj);
    }
    org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue other = (org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue) obj;

    boolean result = true;
    result = result && getValue()
        .equals(other.getValue());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + VALUE_FIELD_NUMBER;
    hash = (53 * hash) + getValue().hashCode();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString data)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString data,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(byte[] data)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
      byte[] data,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
      java.io.InputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseDelimitedFrom(
      java.io.InputStream input,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parseFrom(
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
  public static Builder newBuilder(org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue prototype) {
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
   * Wrapper message for `string`.
   * The JSON representation for `StringValue` is JSON string.
   * </pre>
   *
   * Protobuf type {@code google.protobuf.StringValue}
   */
  public static final class Builder extends
      org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.protobuf.StringValue)
      org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValueOrBuilder {
    public static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.WrappersProto.internal_static_google_protobuf_StringValue_descriptor;
    }

    protected org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.WrappersProto.internal_static_google_protobuf_StringValue_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.class, org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.Builder.class);
    }

    // Construct using org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.newBuilder()
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
      }
    }
    public Builder clear() {
      super.clear();
      value_ = "";

      return this;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.WrappersProto.internal_static_google_protobuf_StringValue_descriptor;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue getDefaultInstanceForType() {
      return org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.getDefaultInstance();
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue build() {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue buildPartial() {
      org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue result = new org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue(this);
      result.value_ = value_;
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
      if (other instanceof org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue) {
        return mergeFrom((org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue other) {
      if (other == org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue.getDefaultInstance()) return this;
      if (!other.getValue().isEmpty()) {
        value_ = other.value_;
        onChanged();
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
      org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object value_ = "";
    /**
     * <pre>
     * The string value.
     * </pre>
     *
     * <code>optional string value = 1;</code>
     */
    public java.lang.String getValue() {
      java.lang.Object ref = value_;
      if (!(ref instanceof java.lang.String)) {
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString bs =
            (org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        value_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     * The string value.
     * </pre>
     *
     * <code>optional string value = 1;</code>
     */
    public org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString
        getValueBytes() {
      java.lang.Object ref = value_;
      if (ref instanceof String) {
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString b = 
            org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        value_ = b;
        return b;
      } else {
        return (org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * The string value.
     * </pre>
     *
     * <code>optional string value = 1;</code>
     */
    public Builder setValue(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      value_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The string value.
     * </pre>
     *
     * <code>optional string value = 1;</code>
     */
    public Builder clearValue() {
      
      value_ = getDefaultInstance().getValue();
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The string value.
     * </pre>
     *
     * <code>optional string value = 1;</code>
     */
    public Builder setValueBytes(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      value_ = value;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final org.apache.hadoop.hbase.shaded.com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:google.protobuf.StringValue)
  }

  // @@protoc_insertion_point(class_scope:google.protobuf.StringValue)
  private static final org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue();
  }

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<StringValue>
      PARSER = new org.apache.hadoop.hbase.shaded.com.google.protobuf.AbstractParser<StringValue>() {
    public StringValue parsePartialFrom(
        org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream input,
        org.apache.hadoop.hbase.shaded.com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException {
        return new StringValue(input, extensionRegistry);
    }
  };

  public static org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<StringValue> parser() {
    return PARSER;
  }

  @java.lang.Override
  public org.apache.hadoop.hbase.shaded.com.google.protobuf.Parser<StringValue> getParserForType() {
    return PARSER;
  }

  public org.apache.hadoop.hbase.shaded.com.google.protobuf.StringValue getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

