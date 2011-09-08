#if WITH_XML
namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using Volante;

    public class XmlImporter
    {
        public XmlImporter(DatabaseImpl db, System.IO.StreamReader reader)
        {
            this.db = db;
            scanner = new XMLScanner(reader);
            classMap = new Hashtable();
        }

        public virtual void importDatabase()
        {
            if (scanner.scan() != XMLScanner.Token.LT || scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("database"))
            {
                throwException("No root element");
            }
            if (scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("root") || scanner.scan() != XMLScanner.Token.EQ || scanner.scan() != XMLScanner.Token.SCONST || scanner.scan() != XMLScanner.Token.GT)
            {
                throwException("Database element should have \"root\" attribute");
            }
            int rootId = 0;
            try
            {
                rootId = System.Int32.Parse(scanner.String);
            }
            catch (System.FormatException)
            {
                throwException("Incorrect root object specification");
            }
            idMap = new int[rootId * 2];
            idMap[rootId] = db.allocateId();
            db.header.root[1 - db.currIndex].rootObject = idMap[rootId];

            XMLScanner.Token tkn;
            while ((tkn = scanner.scan()) == XMLScanner.Token.LT)
            {
                if (scanner.scan() != XMLScanner.Token.IDENT)
                {
                    throwException("Element name expected");
                }
                System.String elemName = scanner.Identifier;
                if (elemName.StartsWith("Volante.Impl.OldBtree")
                    || elemName.StartsWith("Volante.Impl.OldBitIndexImpl")
                    || elemName.StartsWith("Volante.Impl.OldPersistentSet")
                    || elemName.StartsWith("Volante.Impl.OldBtreeFieldIndex")
                    || elemName.StartsWith("Volante.Impl.OldBtreeMultiFieldIndex"))
                {
                    createIndex(elemName);
                }
                else
                {
                    createObject(readElement(elemName));
                }
            }
            if (tkn != XMLScanner.Token.LTS || scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("database") || scanner.scan() != XMLScanner.Token.GT)
            {
                throwException("Root element is not closed");
            }
        }

        internal class XMLElement
        {
            internal XMLElement NextSibling
            {
                get
                {
                    return next;
                }
            }

            internal int Counter
            {
                get
                {
                    return counter;
                }
            }

            internal long IntValue
            {
                get
                {
                    return ivalue;
                }

                set
                {
                    ivalue = value;
                    valueType = XMLValueType.INT_VALUE;
                }
            }

            internal double RealValue
            {
                get
                {
                    return rvalue;
                }

                set
                {
                    rvalue = value;
                    valueType = XMLValueType.REAL_VALUE;
                }
            }

            internal String StringValue
            {
                get
                {
                    return svalue;
                }

                set
                {
                    svalue = value;
                    valueType = XMLValueType.STRING_VALUE;
                }

            }

            internal String Name
            {
                get
                {
                    return name;

                }
            }

            private XMLElement next;
            private XMLElement prev;
            private String name;
            private Hashtable siblings;
            private Hashtable attributes;
            private String svalue;
            private long ivalue;
            private double rvalue;
            private XMLValueType valueType;
            private int counter;

            enum XMLValueType
            {
                NO_VALUE,
                STRING_VALUE,
                INT_VALUE,
                REAL_VALUE,
                NULL_VALUE
            }

            internal XMLElement(System.String name)
            {
                this.name = name;
                valueType = XMLValueType.NO_VALUE;
            }

            internal void addSibling(XMLElement elem)
            {
                if (siblings == null)
                {
                    siblings = new Hashtable();
                }
                XMLElement head = (XMLElement)siblings[elem.name];
                if (head != null)
                {
                    elem.next = null;
                    elem.prev = head.prev;
                    head.prev.next = elem;
                    head.prev = elem;
                    head.counter += 1;
                }
                else
                {
                    elem.prev = elem;
                    siblings[elem.name] = elem;
                    elem.counter = 1;
                }
            }

            internal void addAttribute(System.String name, System.String val)
            {
                if (attributes == null)
                {
                    attributes = new Hashtable();
                }
                attributes[name] = val;
            }

            internal XMLElement getSibling(System.String name)
            {
                if (siblings != null)
                {
                    return (XMLElement)siblings[name];
                }
                return null;
            }

            internal System.String getAttribute(System.String name)
            {
                return attributes != null ? (System.String)attributes[name] : null;
            }

            internal void setNullValue()
            {
                valueType = XMLValueType.NULL_VALUE;
            }

            internal bool isIntValue()
            {
                return valueType == XMLValueType.INT_VALUE;
            }

            internal bool isRealValue()
            {
                return valueType == XMLValueType.REAL_VALUE;
            }

            internal bool isStringValue()
            {
                return valueType == XMLValueType.STRING_VALUE;
            }

            internal bool isNullValue()
            {
                return valueType == XMLValueType.NULL_VALUE;
            }
        }

        internal System.String getAttribute(XMLElement elem, String name)
        {
            System.String val = elem.getAttribute(name);
            if (val == null)
            {
                throwException("Attribute " + name + " is not set");
            }
            return val;
        }

        internal int getIntAttribute(XMLElement elem, String name)
        {
            System.String val = elem.getAttribute(name);
            if (val == null)
            {
                throwException("Attribute " + name + " is not set");
            }
            try
            {
                return System.Int32.Parse(val);
            }
            catch (System.FormatException)
            {
                throwException("Attribute " + name + " should has integer value");
            }
            return -1;
        }

        internal int mapId(int id)
        {
            int oid = 0;
            if (id != 0)
            {
                if (id >= idMap.Length)
                {
                    int[] newMap = new int[id * 2];
                    Array.Copy(idMap, 0, newMap, 0, idMap.Length);
                    idMap = newMap;
                    idMap[id] = oid = db.allocateId();
                }
                else
                {
                    oid = idMap[id];
                    if (oid == 0)
                    {
                        idMap[id] = oid = db.allocateId();
                    }
                }
            }
            return oid;
        }

        internal ClassDescriptor.FieldType mapType(System.String signature)
        {
            try
            {
#if CF
                return (ClassDescriptor.FieldType)ClassDescriptor.parseEnum(typeof(ClassDescriptor.FieldType), signature);
#else
                return (ClassDescriptor.FieldType)Enum.Parse(typeof(ClassDescriptor.FieldType), signature);
#endif
            }
            catch (ArgumentException)
            {
                throwException("Bad type");
                return ClassDescriptor.FieldType.tpObject;
            }
        }

        Key createCompoundKey(ClassDescriptor.FieldType[] types, String[] values)
        {
            ByteBuffer buf = new ByteBuffer();
            int dst = 0;

            for (int i = 0; i < types.Length; i++)
            {
                String val = values[i];
                switch (types[i])
                {
                    case ClassDescriptor.FieldType.tpBoolean:
                        dst = buf.packBool(dst, Int32.Parse(val) != 0);
                        break;

                    case ClassDescriptor.FieldType.tpByte:
                    case ClassDescriptor.FieldType.tpSByte:
                        dst = buf.packI1(dst, Int32.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpChar:
                    case ClassDescriptor.FieldType.tpShort:
                    case ClassDescriptor.FieldType.tpUShort:
                        dst = buf.packI2(dst, Int32.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpInt:
                        dst = buf.packI4(dst, Int32.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpEnum:
                    case ClassDescriptor.FieldType.tpUInt:
                        dst = buf.packI4(dst, (int)UInt32.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpObject:
                    case ClassDescriptor.FieldType.tpOid:
                        dst = buf.packI4(dst, mapId((int)UInt32.Parse(val)));
                        break;

                    case ClassDescriptor.FieldType.tpLong:
                        dst = buf.packI8(dst, Int64.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpULong:
                        dst = buf.packI8(dst, (long)UInt64.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpDate:
                        dst = buf.packDate(dst, DateTime.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpFloat:
                        dst = buf.packF4(dst, Single.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpDouble:
                        dst = buf.packF8(dst, Double.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpDecimal:
                        dst = buf.packDecimal(dst, Decimal.Parse(val));
                        break;

                    case ClassDescriptor.FieldType.tpGuid:
                        dst = buf.packGuid(dst, new Guid(val));
                        break;

                    case ClassDescriptor.FieldType.tpString:
                        dst = buf.packString(dst, val);
                        break;

                    case ClassDescriptor.FieldType.tpArrayOfByte:
                        buf.extend(dst + 4 + (val.Length >> 1));
                        Bytes.pack4(buf.arr, dst, val.Length >> 1);
                        dst += 4;
                        for (int j = 0, n = val.Length; j < n; j += 2)
                        {
                            buf.arr[dst++] = (byte)((getHexValue(val[j]) << 4) | getHexValue(val[j + 1]));
                        }
                        break;
                    default:
                        throwException("Bad key type");
                        break;
                }
            }
            return new Key(buf.toArray());
        }

        Key createKey(ClassDescriptor.FieldType type, String val)
        {
            switch (type)
            {
                case ClassDescriptor.FieldType.tpBoolean:
                    return new Key(Int32.Parse(val) != 0);

                case ClassDescriptor.FieldType.tpByte:
                    return new Key(Byte.Parse(val));

                case ClassDescriptor.FieldType.tpSByte:
                    return new Key(SByte.Parse(val));

                case ClassDescriptor.FieldType.tpChar:
                    return new Key((char)Int32.Parse(val));

                case ClassDescriptor.FieldType.tpShort:
                    return new Key(Int16.Parse(val));

                case ClassDescriptor.FieldType.tpUShort:
                    return new Key(UInt16.Parse(val));

                case ClassDescriptor.FieldType.tpInt:
                    return new Key(Int32.Parse(val));

                case ClassDescriptor.FieldType.tpUInt:
                case ClassDescriptor.FieldType.tpEnum:
                    return new Key(UInt32.Parse(val));

                case ClassDescriptor.FieldType.tpOid:
                    return new Key(ClassDescriptor.FieldType.tpOid, mapId((int)UInt32.Parse(val)));
                case ClassDescriptor.FieldType.tpObject:
                    return new Key(new PersistentStub(db, mapId((int)UInt32.Parse(val))));

                case ClassDescriptor.FieldType.tpLong:
                    return new Key(Int64.Parse(val));

                case ClassDescriptor.FieldType.tpULong:
                    return new Key(UInt64.Parse(val));

                case ClassDescriptor.FieldType.tpFloat:
                    return new Key(Single.Parse(val));

                case ClassDescriptor.FieldType.tpDouble:
                    return new Key(Double.Parse(val));

                case ClassDescriptor.FieldType.tpDecimal:
                    return new Key(Decimal.Parse(val));

                case ClassDescriptor.FieldType.tpGuid:
                    return new Key(new Guid(val));

                case ClassDescriptor.FieldType.tpString:
                    return new Key(val);

                case ClassDescriptor.FieldType.tpArrayOfByte:
                    {
                        byte[] buf = new byte[val.Length >> 1];
                        for (int i = 0; i < buf.Length; i++)
                        {
                            buf[i] = (byte)((getHexValue(val[i * 2]) << 4) | getHexValue(val[i * 2 + 1]));
                        }
                        return new Key(buf);
                    }

                case ClassDescriptor.FieldType.tpDate:
                    return new Key(DateTime.Parse(val));

                default:
                    throwException("Bad key type");
                    break;

            }
            return null;
        }

        internal int parseInt(String str)
        {
            return Int32.Parse(str);
        }

        internal Type findClassByName(String className)
        {
            Type type = (Type)classMap[className];
            if (type == null)
            {
                type = ClassDescriptor.lookup(db, className);
                classMap[className] = type;
            }
            return type;
        }

        internal void createIndex(String indexType)
        {
            XMLScanner.Token tkn;
            int oid = 0;
            bool unique = false;
            String className = null;
            String fieldName = null;
            String[] fieldNames = null;
            long autoinc = 0;
            String type = null;
            while ((tkn = scanner.scan()) == XMLScanner.Token.IDENT)
            {
                System.String attrName = scanner.Identifier;
                if (scanner.scan() != XMLScanner.Token.EQ || scanner.scan() != XMLScanner.Token.SCONST)
                {
                    throwException("Attribute value expected");
                }
                System.String attrValue = scanner.String;
                if (attrName.Equals("id"))
                {
                    oid = mapId(parseInt(attrValue));
                }
                else if (attrName.Equals("unique"))
                {
                    unique = parseInt(attrValue) != 0;
                }
                else if (attrName.Equals("class"))
                {
                    className = attrValue;
                }
                else if (attrName.Equals("type"))
                {
                    type = attrValue;
                }
                else if (attrName.Equals("autoinc"))
                {
                    autoinc = parseInt(attrValue);
                }
                else if (attrName.StartsWith("field"))
                {
                    int len = attrName.Length;
                    if (len == 5)
                    {
                        fieldName = attrValue;
                    }
                    else
                    {
                        int fieldNo = Int32.Parse(attrName.Substring(5));
                        if (fieldNames == null || fieldNames.Length <= fieldNo)
                        {
                            String[] newFieldNames = new String[fieldNo + 1];
                            if (fieldNames != null)
                            {
                                Array.Copy(fieldNames, 0, newFieldNames, 0, fieldNames.Length);
                            }
                            fieldNames = newFieldNames;
                        }
                        fieldNames[fieldNo] = attrValue;
                    }
                }
            }
            if (tkn != XMLScanner.Token.GT)
            {
                throwException("Unclosed element tag");
            }
            if (oid == 0)
            {
                throwException("ID is not specified or index");
            }
            ClassDescriptor desc = db.getClassDescriptor(findClassByName(indexType));
#if WITH_OLD_BTREE
            OldBtree btree = (OldBtree)desc.newInstance();
            if (className != null)
            {
                Type cls = findClassByName(className);
                if (fieldName != null)
                {
                    btree.init(cls, ClassDescriptor.FieldType.tpLast, new string[] { fieldName }, unique, autoinc);
                }
                else if (fieldNames != null)
                {
                    btree.init(cls, ClassDescriptor.FieldType.tpLast, fieldNames, unique, autoinc);
                }
                else
                {
                    throwException("Field name is not specified for field index");
                }
            }
            else
            {
                if (type == null)
                {
                    if (indexType.StartsWith("Volante.Impl.PersistentSet"))
                    {
                    }
                    else
                    {
                        throwException("Key type is not specified for index");
                    }
                }
                else
                {
                    if (indexType.StartsWith("Volante.impl.BitIndexImpl"))
                    {
                    }
                    else
                    {
                        btree.init(null, mapType(type), null, unique, autoinc);
                    }
                }
            }
            db.assignOid(btree, oid);
#endif

            while ((tkn = scanner.scan()) == XMLScanner.Token.LT)
            {
                if (scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("ref"))
                {
                    throwException("<ref> element expected");
                }
#if WITH_OLD_BTREE
                XMLElement refElem = readElement("ref");
                Key key;
                if (fieldNames != null)
                {
                    String[] values = new String[fieldNames.Length];
                    ClassDescriptor.FieldType[] types = btree.FieldTypes;
                    for (int i = 0; i < values.Length; i++)
                    {
                        values[i] = getAttribute(refElem, "key" + i);
                    }
                    key = createCompoundKey(types, values);
                }
                else
                {
                    key = createKey(btree.FieldType, getAttribute(refElem, "key"));
                }
                IPersistent obj = new PersistentStub(db, mapId(getIntAttribute(refElem, "id")));
                btree.insert(key, obj, false);
#endif
            }
            if (tkn != XMLScanner.Token.LTS
                || scanner.scan() != XMLScanner.Token.IDENT
                || !scanner.Identifier.Equals(indexType)
                || scanner.scan() != XMLScanner.Token.GT)
            {
                throwException("Element is not closed");
            }
#if WITH_OLD_BTREE
            ByteBuffer buf = new ByteBuffer();
            buf.extend(ObjectHeader.Sizeof);
            int size = db.packObject(btree, desc, ObjectHeader.Sizeof, buf, null);
            byte[] data = buf.arr;
            ObjectHeader.setSize(data, 0, size);
            ObjectHeader.setType(data, 0, desc.Oid);
            long pos = db.allocate(size, 0);
            db.setPos(oid, pos | DatabaseImpl.dbModifiedFlag);

            db.pool.put(pos & ~DatabaseImpl.dbFlagsMask, data, size);
#endif
        }

        internal void createObject(XMLElement elem)
        {
            ClassDescriptor desc = db.getClassDescriptor(findClassByName(elem.Name));
            int oid = mapId(getIntAttribute(elem, "id"));
            ByteBuffer buf = new ByteBuffer();
            int offs = ObjectHeader.Sizeof;
            buf.extend(offs);

            offs = packObject(elem, desc, offs, buf);

            ObjectHeader.setSize(buf.arr, 0, offs);
            ObjectHeader.setType(buf.arr, 0, desc.Oid);

            long pos = db.allocate(offs, 0);
            db.setPos(oid, pos | DatabaseImpl.dbModifiedFlag);
            db.pool.put(pos, buf.arr, offs);
        }

        internal int getHexValue(char ch)
        {
            if (ch >= '0' && ch <= '9')
            {
                return ch - '0';
            }
            else if (ch >= 'A' && ch <= 'F')
            {
                return ch - 'A' + 10;
            }
            else if (ch >= 'a' && ch <= 'f')
            {
                return ch - 'a' + 10;
            }
            else
            {
                throwException("Bad hexadecimal constant");
            }
            return -1;
        }

        internal int importBinary(XMLElement elem, int offs, ByteBuffer buf, String fieldName)
        {
            if (elem == null || elem.isNullValue())
            {
                buf.extend(offs + 4);
                Bytes.pack4(buf.arr, offs, -1);
                offs += 4;
            }
            else if (elem.isStringValue())
            {
                String hexStr = elem.StringValue;
                int len = hexStr.Length;
                if (hexStr.StartsWith("#"))
                {
                    buf.extend(offs + 4 + len / 2 - 1);
                    Bytes.pack4(buf.arr, offs, -2 - getHexValue(hexStr[1]));
                    offs += 4;
                    for (int j = 2; j < len; j += 2)
                    {
                        buf.arr[offs++] = (byte)((getHexValue(hexStr[j]) << 4) | getHexValue(hexStr[j + 1]));
                    }
                }
                else
                {
                    buf.extend(offs + 4 + len / 2);
                    Bytes.pack4(buf.arr, offs, len / 2);
                    offs += 4;
                    for (int j = 0; j < len; j += 2)
                    {
                        buf.arr[offs++] = (byte)((getHexValue(hexStr[j]) << 4) | getHexValue(hexStr[j + 1]));
                    }
                }
            }
            else
            {
                XMLElement refElem = elem.getSibling("ref");
                if (refElem != null)
                {
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, mapId(getIntAttribute(refElem, "id")));
                    offs += 4;
                }
                else
                {
                    XMLElement item = elem.getSibling("element");
                    int len = (item == null) ? 0 : item.Counter;
                    buf.extend(offs + 4 + len);
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    while (--len >= 0)
                    {
                        if (item.isIntValue())
                        {
                            buf.arr[offs] = (byte)item.IntValue;
                        }
                        else if (item.isRealValue())
                        {
                            buf.arr[offs] = (byte)item.RealValue;
                        }
                        else
                        {
                            throwException("Conversion for field " + fieldName + " is not possible");
                        }
                        item = item.NextSibling;
                        offs += 1;
                    }
                }
            }
            return offs;
        }

        internal int packObject(XMLElement objElem, ClassDescriptor desc, int offs, ByteBuffer buf)
        {
            ClassDescriptor.FieldDescriptor[] flds = desc.allFields;
            for (int i = 0, n = flds.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = flds[i];
                FieldInfo f = fd.field;
                String fieldName = fd.fieldName;
                XMLElement elem = (objElem != null) ? objElem.getSibling(fieldName) : null;

                switch (fd.type)
                {
                    case ClassDescriptor.FieldType.tpByte:
                    case ClassDescriptor.FieldType.tpSByte:
                        buf.extend(offs + 1);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                buf.arr[offs] = (byte)elem.IntValue;
                            }
                            else if (elem.isRealValue())
                            {
                                buf.arr[offs] = (byte)elem.RealValue;
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 1;
                        continue;

                    case ClassDescriptor.FieldType.tpBoolean:
                        buf.extend(offs + 1);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                buf.arr[offs] = (byte)(elem.IntValue != 0 ? 1 : 0);
                            }
                            else if (elem.isRealValue())
                            {
                                buf.arr[offs] = (byte)(elem.RealValue != 0.0 ? 1 : 0);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 1;
                        continue;

                    case ClassDescriptor.FieldType.tpShort:
                    case ClassDescriptor.FieldType.tpUShort:
                    case ClassDescriptor.FieldType.tpChar:
                        buf.extend(offs + 2);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.pack2(buf.arr, offs, (short)elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack2(buf.arr, offs, (short)elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 2;
                        continue;

                    case ClassDescriptor.FieldType.tpEnum:
                        buf.extend(offs + 4);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int)elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int)elem.RealValue);
                            }
                            else if (elem.isStringValue())
                            {
                                try
                                {
#if CF
                                    Bytes.pack4(buf.arr, offs, (int)ClassDescriptor.parseEnum(f.FieldType, elem.StringValue));
#else
                                    Bytes.pack4(buf.arr, offs, (int)Enum.Parse(f.FieldType, elem.StringValue));
#endif
                                }
                                catch (ArgumentException)
                                {
                                    throwException("Invalid enum value");
                                }
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 4;
                        continue;

                    case ClassDescriptor.FieldType.tpInt:
                    case ClassDescriptor.FieldType.tpUInt:
                        buf.extend(offs + 4);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int)elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int)elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 4;
                        continue;

                    case ClassDescriptor.FieldType.tpLong:
                    case ClassDescriptor.FieldType.tpULong:
                        buf.extend(offs + 8);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.pack8(buf.arr, offs, elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack8(buf.arr, offs, (long)elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 8;
                        continue;

                    case ClassDescriptor.FieldType.tpFloat:
                        buf.extend(offs + 4);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.packF4(buf.arr, offs, (float)elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.packF4(buf.arr, offs, (float)elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 4;
                        continue;

                    case ClassDescriptor.FieldType.tpDouble:
                        buf.extend(offs + 8);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.packF8(buf.arr, offs, (double)elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.packF8(buf.arr, offs, elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 8;
                        continue;

                    case ClassDescriptor.FieldType.tpDecimal:
                        buf.extend(offs + 16);
                        if (elem != null)
                        {
                            decimal d = 0;
                            if (elem.isIntValue())
                            {
                                d = elem.IntValue;
                            }
                            else if (elem.isRealValue())
                            {
                                d = (decimal)elem.RealValue;
                            }
                            else if (elem.isStringValue())
                            {
                                try
                                {
                                    d = Decimal.Parse(elem.StringValue);
                                }
                                catch (FormatException)
                                {
                                    throwException("Invalid date");
                                }
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                            Bytes.packDecimal(buf.arr, offs, d);

                        }
                        offs += 16;
                        continue;

                    case ClassDescriptor.FieldType.tpGuid:
                        buf.extend(offs + 16);
                        if (elem != null)
                        {
                            if (elem.isStringValue())
                            {
                                Guid guid = new Guid(elem.StringValue);
                                byte[] bits = guid.ToByteArray();
                                Array.Copy(bits, 0, buf.arr, offs, 16);
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 16;
                        continue;

                    case ClassDescriptor.FieldType.tpDate:
                        buf.extend(offs + 8);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                Bytes.pack8(buf.arr, offs, elem.IntValue);
                            }
                            else if (elem.isNullValue())
                            {
                                Bytes.pack8(buf.arr, offs, -1);
                            }
                            else if (elem.isStringValue())
                            {
                                try
                                {
                                    Bytes.packDate(buf.arr, offs, DateTime.Parse(elem.StringValue));
                                }
                                catch (FormatException)
                                {
                                    throwException("Invalid date");
                                }
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                        }
                        offs += 8;
                        continue;

                    case ClassDescriptor.FieldType.tpString:
                        if (elem != null)
                        {
                            System.String val = null;
                            if (elem.isIntValue())
                            {
                                val = System.Convert.ToString(elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                val = elem.RealValue.ToString();
                            }
                            else if (elem.isStringValue())
                            {
                                val = elem.StringValue;
                            }
                            else if (elem.isNullValue())
                            {
                                val = null;
                            }
                            else
                            {
                                throwException("Conversion for field " + fieldName + " is not possible");
                            }
                            offs = buf.packString(offs, val);
                            continue;
                        }
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                        continue;

                    case ClassDescriptor.FieldType.tpOid:
                    case ClassDescriptor.FieldType.tpObject:
                        {
                            int oid = 0;
                            if (elem != null)
                            {
                                XMLElement refElem = elem.getSibling("ref");
                                if (refElem == null)
                                {
                                    throwException("<ref> element expected");
                                }
                                oid = mapId(getIntAttribute(refElem, "id"));
                            }
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, oid);
                            offs += 4;
                            continue;
                        }

                    case ClassDescriptor.FieldType.tpValue:
                        offs = packObject(elem, fd.valueDesc, offs, buf);
                        continue;

                    case ClassDescriptor.FieldType.tpRaw:
                    case ClassDescriptor.FieldType.tpArrayOfByte:
                    case ClassDescriptor.FieldType.tpArrayOfSByte:
                        offs = importBinary(elem, offs, buf, fieldName);
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfBoolean:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    buf.arr[offs] = (byte)(item.IntValue != 0 ? 1 : 0);
                                }
                                else if (item.isRealValue())
                                {
                                    buf.arr[offs] = (byte)(item.RealValue != 0.0 ? 1 : 0);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 1;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfChar:
                    case ClassDescriptor.FieldType.tpArrayOfShort:
                    case ClassDescriptor.FieldType.tpArrayOfUShort:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 2);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack2(buf.arr, offs, (short)item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack2(buf.arr, offs, (short)item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 2;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfEnum:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            Type elemType = f.FieldType.GetElementType();
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int)item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int)item.RealValue);
                                }
                                else if (item.isStringValue())
                                {
                                    try
                                    {
#if CF
                                        Bytes.pack4(buf.arr, offs, (int)ClassDescriptor.parseEnum(elemType, item.StringValue));
#else
                                        Bytes.pack4(buf.arr, offs, (int)Enum.Parse(elemType, item.StringValue));
#endif
                                    }
                                    catch (ArgumentException)
                                    {
                                        throwException("Invalid enum value");
                                    }
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 4;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfInt:
                    case ClassDescriptor.FieldType.tpArrayOfUInt:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int)item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int)item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 4;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfLong:
                    case ClassDescriptor.FieldType.tpArrayOfULong:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack8(buf.arr, offs, item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack8(buf.arr, offs, (long)item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 8;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfFloat:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.packF4(buf.arr, offs, (float)item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.packF4(buf.arr, offs, (float)item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 4;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfDouble:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.packF8(buf.arr, offs, (double)item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.packF8(buf.arr, offs, item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 8;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfDate:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isNullValue())
                                {
                                    Bytes.pack8(buf.arr, offs, -1);
                                }
                                else if (item.isStringValue())
                                {
                                    try
                                    {
                                        Bytes.packDate(buf.arr, offs, DateTime.Parse(item.StringValue));
                                    }
                                    catch (FormatException)
                                    {
                                        throwException("Conversion for field " + fieldName + " is not possible");
                                    }
                                }
                                item = item.NextSibling;
                                offs += 8;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfDecimal:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 16);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isStringValue())
                                {
                                    try
                                    {
                                        Bytes.packDecimal(buf.arr, offs, Decimal.Parse(item.StringValue));
                                    }
                                    catch (FormatException)
                                    {
                                        throwException("Conversion for field " + fieldName + " is not possible");
                                    }
                                }
                                item = item.NextSibling;
                                offs += 16;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfGuid:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 16);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isStringValue())
                                {
                                    try
                                    {
                                        Bytes.packGuid(buf.arr, offs, new Guid(item.StringValue));
                                    }
                                    catch (FormatException)
                                    {
                                        throwException("Conversion for field " + fieldName + " is not possible");
                                    }
                                }
                                item = item.NextSibling;
                                offs += 16;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfString:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                System.String val = null;
                                if (item.isIntValue())
                                {
                                    val = System.Convert.ToString(item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    val = item.RealValue.ToString();
                                }
                                else if (item.isStringValue())
                                {
                                    val = item.StringValue;
                                }
                                else if (item.isNullValue())
                                {
                                    val = null;
                                }
                                else
                                {
                                    throwException("Conversion for field " + fieldName + " is not possible");
                                }
                                offs = buf.packString(offs, val);
                                item = item.NextSibling;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfObject:
                    case ClassDescriptor.FieldType.tpArrayOfOid:
                    case ClassDescriptor.FieldType.tpLink:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                XMLElement href = item.getSibling("ref");
                                if (href == null)
                                {
                                    throwException("<ref> element expected");
                                }
                                int oid = mapId(getIntAttribute(href, "id"));
                                Bytes.pack4(buf.arr, offs, oid);
                                item = item.NextSibling;
                                offs += 4;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfValue:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            ClassDescriptor elemDesc = fd.valueDesc;
                            while (--len >= 0)
                            {
                                offs = packObject(item, elemDesc, offs, buf);
                                item = item.NextSibling;
                            }
                        }
                        continue;

                    case ClassDescriptor.FieldType.tpArrayOfRaw:
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, -1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("element");
                            int len = (item == null) ? 0 : item.Counter;
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                offs = importBinary(item, offs, buf, fieldName);
                                item = item.NextSibling;
                            }
                        }
                        continue;
                }
            }
            return offs;
        }

        internal XMLElement readElement(System.String name)
        {
            XMLElement elem = new XMLElement(name);
            System.String attribute;
            XMLScanner.Token tkn;
            while (true)
            {
                switch (scanner.scan())
                {
                    case XMLScanner.Token.GTS:
                        return elem;

                    case XMLScanner.Token.GT:
                        while ((tkn = scanner.scan()) == XMLScanner.Token.LT)
                        {
                            if (scanner.scan() != XMLScanner.Token.IDENT)
                            {
                                throwException("Element name expected");
                            }
                            System.String siblingName = scanner.Identifier;
                            XMLElement sibling = readElement(siblingName);
                            elem.addSibling(sibling);
                        }
                        switch (tkn)
                        {
                            case XMLScanner.Token.SCONST:
                                elem.StringValue = scanner.String;
                                tkn = scanner.scan();
                                break;

                            case XMLScanner.Token.ICONST:
                                elem.IntValue = scanner.Int;
                                tkn = scanner.scan();
                                break;

                            case XMLScanner.Token.FCONST:
                                elem.RealValue = scanner.Real;
                                tkn = scanner.scan();
                                break;

                            case XMLScanner.Token.IDENT:
                                if (scanner.Identifier.Equals("null"))
                                {
                                    elem.setNullValue();
                                }
                                else
                                {
                                    elem.StringValue = scanner.Identifier;
                                }
                                tkn = scanner.scan();
                                break;

                        }
                        if (tkn != XMLScanner.Token.LTS || scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals(name) || scanner.scan() != XMLScanner.Token.GT)
                        {
                            throwException("Element is not closed");
                        }
                        return elem;

                    case XMLScanner.Token.IDENT:
                        attribute = scanner.Identifier;
                        if (scanner.scan() != XMLScanner.Token.EQ || scanner.scan() != XMLScanner.Token.SCONST)
                        {
                            throwException("Attribute value expected");
                        }
                        elem.addAttribute(attribute, scanner.String);
                        continue;

                    default:
                        throwException("Unexpected token");
                        break;

                }
            }
        }

        internal void throwException(System.String message)
        {
            throw new XmlImportException(scanner.Line, scanner.Column, message);
        }

        internal DatabaseImpl db;
        internal XMLScanner scanner;
        internal Hashtable classMap;
        internal int[] idMap;

        internal class XMLScanner
        {
            internal virtual System.String Identifier
            {
                get
                {
                    return ident;
                }
            }

            internal virtual System.String String
            {
                get
                {
                    return new String(sconst, 0, slen);
                }
            }

            internal virtual long Int
            {
                get
                {
                    return iconst;
                }
            }

            internal virtual double Real
            {
                get
                {
                    return fconst;
                }
            }

            internal virtual int Line
            {
                get
                {
                    return line;
                }
            }

            internal virtual int Column
            {
                get
                {
                    return column;
                }
            }

            internal enum Token
            {
                IDENT,
                SCONST,
                ICONST,
                FCONST,
                LT,
                GT,
                LTS,
                GTS,
                EQ,
                EOF
            }

            internal System.IO.StreamReader reader;
            internal int line;
            internal int column;
            internal char[] sconst;
            internal long iconst;
            internal double fconst;
            internal int slen;
            internal String ident;
            internal int size;
            internal int ungetChar;
            internal bool hasUngetChar;

            internal XMLScanner(System.IO.StreamReader reader)
            {
                this.reader = reader;
                sconst = new char[size = 1024];
                line = 1;
                column = 0;
                hasUngetChar = false;
            }

            internal int get()
            {
                if (hasUngetChar)
                {
                    hasUngetChar = false;
                    return ungetChar;
                }
                int ch = reader.Read();
                if (ch == '\n')
                {
                    line += 1;
                    column = 0;
                }
                else if (ch == '\t')
                {
                    column += (column + 8) & ~7;
                }
                else
                {
                    column += 1;
                }
                return ch;
            }

            internal void unget(int ch)
            {
                if (ch == '\n')
                {
                    line -= 1;
                }
                else
                {
                    column -= 1;
                }
                ungetChar = ch;
                hasUngetChar = true;
            }

            internal Token scan()
            {
                int i, ch;
                bool floatingPoint;

                while (true)
                {
                    do
                    {
                        if ((ch = get()) < 0)
                        {
                            return Token.EOF;
                        }
                    }
                    while (ch <= ' ');

                    switch (ch)
                    {
                        case '<':
                            ch = get();
                            if (ch == '?')
                            {
                                while ((ch = get()) != '?')
                                {
                                    if (ch < 0)
                                    {
                                        throw new XmlImportException(line, column, "Bad XML file format");
                                    }
                                }
                                if ((ch = get()) != '>')
                                {
                                    throw new XmlImportException(line, column, "Bad XML file format");
                                }
                                continue;
                            }
                            if (ch != '/')
                            {
                                unget(ch);
                                return Token.LT;
                            }
                            return Token.LTS;

                        case '>':
                            return Token.GT;

                        case '/':
                            ch = get();
                            if (ch != '>')
                            {
                                unget(ch);
                                throw new XmlImportException(line, column, "Bad XML file format");
                            }
                            return Token.GTS;

                        case '=':
                            return Token.EQ;

                        case '"':
                            i = 0;
                            while (true)
                            {
                                ch = get();
                                if (ch < 0)
                                {
                                    throw new XmlImportException(line, column, "Bad XML file format");
                                }
                                else if (ch == '&')
                                {
                                    switch (get())
                                    {
                                        case 'a':
                                            if (get() != 'm' || get() != 'p' || get() != ';')
                                            {
                                                throw new XmlImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '&';
                                            break;

                                        case 'l':
                                            if (get() != 't' || get() != ';')
                                            {
                                                throw new XmlImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '<';
                                            break;

                                        case 'g':
                                            if (get() != 't' || get() != ';')
                                            {
                                                throw new XmlImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '>';
                                            break;

                                        case 'q':
                                            if (get() != 'u' || get() != 'o' || get() != 't' || get() != ';')
                                            {
                                                throw new XmlImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '"';
                                            break;

                                        default:
                                            throw new XmlImportException(line, column, "Bad XML file format");

                                    }
                                }
                                else if (ch == '"')
                                {
                                    slen = i;
                                    return Token.SCONST;
                                }
                                if (i == size)
                                {
                                    char[] newBuf = new char[size *= 2];
                                    Array.Copy(sconst, 0, newBuf, 0, i);
                                    sconst = newBuf;
                                }
                                sconst[i++] = (char)ch;
                            }

                        case '-':
                        case '0':
                        case '1':
                        case '2':
                        case '3':
                        case '4':
                        case '5':
                        case '6':
                        case '7':
                        case '8':
                        case '9':
                            i = 0;
                            floatingPoint = false;
                            while (true)
                            {
                                if (!System.Char.IsDigit((char)ch) && ch != '-' && ch != '+' && ch != '.' && ch != 'E')
                                {
                                    unget(ch);
                                    try
                                    {
                                        if (floatingPoint)
                                        {
                                            fconst = System.Double.Parse(new String(sconst, 0, i));
                                            return Token.FCONST;
                                        }
                                        else
                                        {
                                            iconst = sconst[0] == '-' ? System.Int64.Parse(new String(sconst, 0, i))
                                                : (long)System.UInt64.Parse(new String(sconst, 0, i));
                                            return Token.ICONST;
                                        }
                                    }
                                    catch (System.FormatException)
                                    {
                                        throw new XmlImportException(line, column, "Bad XML file format");
                                    }
                                }
                                if (i == size)
                                {
                                    throw new XmlImportException(line, column, "Bad XML file format");
                                }
                                sconst[i++] = (char)ch;
                                if (ch == '.')
                                {
                                    floatingPoint = true;
                                }
                                ch = get();
                            }

                        default:
                            i = 0;
                            while (System.Char.IsLetterOrDigit((char)ch) || ch == '-' || ch == ':' || ch == '_' || ch == '.')
                            {
                                if (i == size)
                                {
                                    throw new XmlImportException(line, column, "Bad XML file format");
                                }
                                if (ch == '-')
                                {
                                    ch = '+';
                                }
                                sconst[i++] = (char)ch;
                                ch = get();
                            }
                            unget(ch);
                            if (i == 0)
                            {
                                throw new XmlImportException(line, column, "Bad XML file format");
                            }
                            ident = new String(sconst, 0, i);
                            ident = ident.Replace(".1", "`");
                            ident = ident.Replace(".2", ",");
                            ident = ident.Replace(".3", "[");
                            ident = ident.Replace(".4", "]");
                            ident = ident.Replace(".5", "=");
                            return Token.IDENT;
                    }
                }
            }
        }
    }
}
#endif

