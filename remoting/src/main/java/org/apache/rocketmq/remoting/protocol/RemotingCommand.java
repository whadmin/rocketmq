/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算机网络通讯 通过byte[]进行相互，我们需要设计客户端和服务交互的协议
 *
 * 如下
 * <length> <header length> <header data> <body data>
 *  4个字节    4个字节
 * 服务器与客户端通过传递 如上协议进行交互，
 *
  length：4个字节的int型数据，用来存储header length、header data、body data的总和，也就是网络传输包的数据总长
  header length：4个字节的int型数据，前一个字节用存储序列号类型  在MQ中序列话的对象是RemotingCommand 我们会把RemotingCommand序列化为2进制存储到header data中
  序列化的方式有2种，一种是JSON 一种是ROCKETMQ  后3个字节用来存储header data的长度。
  这里有点难以理解点在于RemotingCommand描述的是header data对象。但其中还存在body,customHeader属性
  这样RemotingCommand不仅仅描述了header data 同时也包含了body data，当我们在encode()的时候 可以认为 header data 》 body data  body data还必须存在于RemotingCommand属性中
  header data：存储报文头部的数据
  body data：存储报文体的数据  一个2进制
 *
 * RemotingCommand  encode()行为是针对RemotingCommand 编码为<length> <header length> <header data> <body data> 格式的二进制包
 *
 * RemotingCommand decode行为只针对 <header length> <header data> <body data> 格式解码。因为
 * NettyDecoder extends LengthFieldBasedFrameDecoder 帮我们解析的时候去掉了 <length>
 */
public class RemotingCommand {

    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";

    /**
     * rpc类型的标注，一种是普通的RPC请求{请求等待回应} 请求类型为RPC_TYPE 这里的0 表示2的0次方
     */
    private static final int RPC_TYPE = 0;
    /**
     * rpc类型的标注，一种是单向RPC请求{请求无须回应} 返回类型或者无需应答的为 这里的1 表示2的1次方
     */
    private static final int RPC_ONEWAY = 1;

    /**
     * CommandCustomHader是所有 headerdata【协议头数据】都要实现的接口，后面的Field[]就是对应的headerdata实现接口类的成员属性
     * map就是解析时候的不同CommandCustomHader之类对应属性的数据字典
     */
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
            new HashMap<Class<? extends CommandCustomHeader>, Field[]>();

    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();

    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<Field, Boolean>();


    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();

    private static volatile int configVersion = -1;
    /**
     * 这里的requestId是RPC请求的序号，每次请求的时候都会increment一下
     *
     */
    private static AtomicInteger requestId = new AtomicInteger(0);
    private int opaque = requestId.getAndIncrement();

    /**
     * 请求或响应代码
     *
     * Request:  请求方会根据不同code 表示不同的请求操作。
     * Response: code=0 表示成功,非0表示不同类型错误代码
     */
    private int code;


    /**
     *区分语言种类
     * Request:  请求方默认的实现语言。 默认JAVA
     * Response: 响应方默认的实现语言。 默认JAVA
     */
    private LanguageCode language = LanguageCode.JAVA;

    /**
     * RPC版本号
     */
    private int version = 0;

    /**
     * 0 一种是普通的RPC request类型{请求等待回应}  1 3 表示Response类型{无须回应}  2  一种是单向RPC请求{请求无须回应}
     */
    private int flag = 0;

    /**
     * 标注信息
     */
    private String remark;

    /**
     * 存放本次RPC通信中所有的extFeilds，extFeilds其实就可以理解成本次通信CommandCustomHeader实现Map存储格式
     */
    private HashMap<String, String> extFields;

    /**
     * 包头数据，注意transient标记，不会被序列化
     */
    private transient CommandCustomHeader customHeader;

    /**
     * body data 数据包数据
     */
    private transient byte[] body;

    /**
     * 序列化的方式，JSON 或者 ROCKETMQ
     *
     */
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;


    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    protected RemotingCommand() {
    }

    /**
     * 从configVersion，System.getProperty(REMOTING_VERSION_KEY) 获取版本号
     * 设置到 RemotingCommand version
     * @param cmd
     */
    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    /**
     * 创建一个Request类型的 RemotingCommand
     * headerdata 对象类型为 classHeader,
     * code Request类型编码为 code
     * @param code
     * @param customHeader
     * @return
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * 创建一个返回类型的 RemotingCommand
     * headerdata 对象类型为 classHeader,
     * code 返回编码为 RemotingSysResponseCode.SYSTEM_ERROR
     * remark 备注为  "not set any response code"
     * @param classHeader
     * @return
     */
    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    /**
     * 创建一个Response类型的 RemotingCommand
     * headerdata 对象类型为 null
     * code 返回编码为 code
     * remark 备注为  remark
     * @param code  请求编
     * @param remark  备注
     * @return
     */
    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    /**
     * 创建一个Response类型类型的 RemotingCommand
     * @param remark
     * @param classHeader
     * @return
     */
    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }


    /**
     * 通过<header length> 整数【4个字节表示】 后3个字节获得<header data>序列化类型
     * @param length
     * @return
     */
    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }


    /**
     * 对头部二进制解码为 RemotingCommand
     * @param headerData
     * @param type
     * @return
     */
    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }


    /**
     * 通过<header length> 整数【4个字节表示】 第1个字节获得<header data>序列化方式
     * @param source
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }


    public static int createNewRequestId() {
        return requestId.incrementAndGet();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }


    /**
     * 编码 <header length>
     * @param source 头部长度
     * @param type  序列化类型
     * @return
     */
    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }


    /**
     * 获取RemotingCommand 类型
     *
     * flag =0 表示请求类型  flag =1 表示返回类型
     *
     */
    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public CommandCustomHeader decodeCommandCustomHeader(
        Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }

        if (this.extFields != null) {

            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    /**
     * 将RemotingCommand编码 转化为协议 <length> <header length> <header data> <body data>
     * @return
     */
    public ByteBuffer encode() {
        // 1> <header length> 占用4个字节
        int length = 4;

        // 2> 针对当前对象RemotingCommand序列化为2进制数组 表示协议中的 <header data>.
        // 获取2进制数组的长度累加到length length 表示 <header length> <header data> 占用的总字节
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> RemotingCommand.body表示协议中<body data>
        // 获取body的长度累加到length length 表示 <header length> <header data> <body data> 占用的总字节
        if (this.body != null) {
            length += body.length;
        }

        //分配空间 4 表示协议中<length>占用的  length 表示 <header length> <header data> <body data> 占用的总字节
        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // 添加 length
        result.putInt(length);

        // 添加 header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // 添加 header data
        result.put(headerData);

        // 添加 body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    /**
     * 将协议 <header length> <header data> <body data> 转化为 RemotingCommand
     * 协议 <length> 部分被NettyDecoder 去掉
     * @param array
     * @return
     */
    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        //获取byteBuffer 总长度 表示 <header length> <header data> <body data> 占用的字节总数
        int length = byteBuffer.limit();
        //获取前面4个字节 <header length> 的整数表示 整数的第1个字节表示序列化类型，后面3个字节表示<header data>长度
        int oriHeaderLen = byteBuffer.getInt();
        //通过<header length> 整数【4个字节表示】 第1个字节获得<header data>长度
        int headerLength = getHeaderLength(oriHeaderLen);

        //获取<header data> 字节数组
        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        //通过<header length> 整数【4个字节表示】 第1个字节获得<header data>序列化方式
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        //获取body
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<String, String>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
}
