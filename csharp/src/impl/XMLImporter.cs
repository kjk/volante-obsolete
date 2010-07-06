namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    public class XMLImporter
    {
        public XMLImporter(StorageImpl storage, System.IO.StreamReader reader)
        {
            this.storage = storage;
            scanner = new XMLScanner(reader);
            classMap = new Hashtable();
        }
		
        public virtual void  importDatabase()
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
            catch (System.FormatException x)
            {
                throwException("Incorrect root object specification");
            }
            idMap = new int[rootId * 2];
            idMap[rootId] = storage.allocateId();
            storage.header.root[1 - storage.currIndex].rootObject = idMap[rootId];
			
            XMLElement elem;
            XMLScanner.Token tkn;
            while ((tkn = scanner.scan()) == XMLScanner.Token.LT)
            {
                if (scanner.scan() != XMLScanner.Token.IDENT)
                {
                    throwException("Element name expected");
                }
                System.String elemName = scanner.Identifier;
                if (elemName.Equals("btree-index"))
                {
                    createIndex();
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
                    valueType = ValueType.INT_VALUE;
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
                    valueType = ValueType.REAL_VALUE;
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
                    valueType = ValueType.STRING_VALUE;
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
            private String     name;
            private Hashtable  siblings;
            private Hashtable  attributes;
            private String     svalue;
            private long       ivalue;
            private double     rvalue;
            private ValueType  valueType;
            private int        counter;
			
            enum ValueType 
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
                valueType = ValueType.NO_VALUE;
            }
			
            internal void  addSibling(XMLElement elem)
            {
                if (siblings == null)
                {
                    siblings = new Hashtable();
                }
                XMLElement head = (XMLElement) siblings[elem.name];
                if (head != null)
                {
                    elem.next = head;
                    elem.prev = head.prev;
                    head.prev.next = elem;
                    head.prev = elem;
                    head.counter += 1;
                }
                else
                {
                    elem.next = elem.prev = elem;
                    siblings[elem.name] = elem;
                    elem.counter = 1;
                }
            }
			
            internal void  addAttribute(System.String name, System.String val)
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
                return attributes != null?(System.String) attributes[name]:null;
            }
			
			
			
			
            internal void  setNullValue()
            {
                valueType = ValueType.NULL_VALUE;
            }
			
            internal bool isIntValue()
            {
                return valueType == ValueType.INT_VALUE;
            }
			
            internal bool isRealValue()
            {
                return valueType == ValueType.REAL_VALUE;
            }
			
            internal bool isStringValue()
            {
                return valueType == ValueType.STRING_VALUE;
            }
			
            internal bool isNullValue()
            {
                return valueType == ValueType.NULL_VALUE;
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
            catch (System.FormatException x)
            {
                throwException("Attribute " + name + " should has integer value");
            }
            return - 1;
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
                    idMap[id] = oid = storage.allocateId();
                }
                else
                {
                    oid = idMap[id];
                    if (oid == 0)
                    {
                        idMap[id] = oid = storage.allocateId();
                    }
                }
            }
            return oid;
        }
		
        internal ClassDescriptor.FieldType mapType(System.String signature)
        {
            try 
            { 
                return (ClassDescriptor.FieldType)Enum.Parse(typeof(ClassDescriptor.FieldType), signature);
            } 
            catch (ArgumentException x) 
            {
                throwException("Bad type");
                return ClassDescriptor.FieldType.tpObject;
            }
        }
		
        internal Key createKey(ClassDescriptor.FieldType type, System.String val)
        {
            try
            {
                IPersistent obj;
                switch (type)
                {
                    case ClassDescriptor.FieldType.tpBoolean: 
                        return new Key(System.Int32.Parse(val) != 0);
					
                    case ClassDescriptor.FieldType.tpByte: 
                        return new Key((byte) System.Int32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpSByte: 
                        return new Key((sbyte) System.Int32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpChar: 
                        return new Key((char) System.Int32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpShort: 
                        return new Key(System.UInt32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpUShort: 
                        return new Key((ushort) System.Int32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpInt: 
                        return new Key(System.Int32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpUInt: 
                    case ClassDescriptor.FieldType.tpEnum:
                        return new Key(System.UInt32.Parse(val));
					
                    case ClassDescriptor.FieldType.tpObject: 
                        obj = new Persistent();
                        storage.assignOid(obj, mapId(System.Int32.Parse(val)));
                        return new Key(obj);
					
                    case ClassDescriptor.FieldType.tpLong: 
                        return new Key(System.Int64.Parse(val));
					
                    case ClassDescriptor.FieldType.tpULong: 
                        return new Key(System.UInt64.Parse(val));
					
                    case ClassDescriptor.FieldType.tpFloat: 
                        return new Key(System.Single.Parse(val));
					
                    case ClassDescriptor.FieldType.tpDouble: 
                        return new Key(System.Double.Parse(val));
					
                    case ClassDescriptor.FieldType.tpString: 
                        return new Key(val);
					
                    case ClassDescriptor.FieldType.tpDate: 
                        return new Key(DateTime.Parse(val));
					
                    default: 
                        throwException("Bad key type");
                        break;
					
                }
            }
            catch (System.FormatException x)
            {
                throwException("Failed to convert key value");
            }
            return null;
        }
		
        internal int parseInt(String str)
        {
            try
            {
                return System.Int32.Parse(str);
            }
            catch (FormatException x)
            {
                throwException("Bad integer constant");
            }
            return -1;
        }
		
        internal Type findClassByName(String className) 
        {
            Type type = (Type)classMap[className];
            if (type == null) 
            {
                type = ClassDescriptor.lookup(className);
                classMap[className] = type;
            }
            return type;
        }

        internal void  createIndex()
        {
            Btree btree;
            XMLScanner.Token tkn;
            int oid = 0;
            bool unique = false;
            System.String className = null;
            System.String fieldName = null;
            System.String type = null;
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
                else if (attrName.Equals("field"))
                {
                    fieldName = attrValue;
                }
                else if (attrName.Equals("type"))
                {
                    type = attrValue;
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
            if (className != null)
            {
                if (fieldName == null)
                {
                    throwException("Field name is not specified for field index");
                }
                btree = new BtreeFieldIndex(findClassByName(className), fieldName, unique);
            }
            else
            {
                if (type == null)
                {
                    throwException("Key type is not specified for index");
                }
                btree = new Btree(mapType(type), unique);
            }
            storage.assignOid(btree, oid);
			
            while ((tkn = scanner.scan()) == XMLScanner.Token.LT)
            {
                if (scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("ref"))
                {
                    throwException("<ref> element expected");
                }
                XMLElement refElem = readElement("ref");
                System.String entryKey = getAttribute(refElem, "key");
                int entryOid = mapId(getIntAttribute(refElem, "id"));
                Key key = createKey(btree.type, entryKey);
                IPersistent obj = new Persistent();
                storage.assignOid(obj, entryOid);
                btree.insert(key, obj, false);
            }
            if (tkn != XMLScanner.Token.LTS || scanner.scan() != XMLScanner.Token.IDENT || !scanner.Identifier.Equals("btree-index") || scanner.scan() != XMLScanner.Token.GT)
            {
                throwException("Element is not closed");
            }
            byte[] data = storage.packObject(btree);
            int size = ObjectHeader.getSize(data, 0);
            long pos = storage.allocate(size, 0);
            storage.setPos(oid, pos | StorageImpl.dbModifiedFlag);
			
            storage.pool.put(pos & ~ StorageImpl.dbFlagsMask, data, size);
        }
		
        internal void  createObject(XMLElement elem)
        {
            ClassDescriptor desc = storage.getClassDescriptor(findClassByName(elem.Name));
            int oid = mapId(getIntAttribute(elem, "id"));
            ByteBuffer buf = new ByteBuffer();
            int offs = ObjectHeader.Sizeof;
            buf.extend(offs);
			
            offs = packObject(elem, desc, offs, buf);
			
            ObjectHeader.setSize(buf.arr, 0, offs);
            ObjectHeader.setType(buf.arr, 0, desc.Oid);
			
            long pos = storage.allocate(offs, 0);
            storage.setPos(oid, pos | StorageImpl.dbModifiedFlag);
            storage.pool.put(pos, buf.arr, offs);
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
            return - 1;
        }
		
        internal int packObject(XMLElement objElem, ClassDescriptor desc, int offs, ByteBuffer buf)
        {
            System.Reflection.FieldInfo[] flds = desc.allFields;
            ClassDescriptor.FieldType[] types = desc.fieldTypes;
            for (int i = 0, n = flds.Length; i < n; i++)
            {
                System.Reflection.FieldInfo f = flds[i];
                XMLElement elem = (objElem != null)?objElem.getSibling(f.Name):null;
				
                switch (types[i])
                {
                    case ClassDescriptor.FieldType.tpByte: 
                    case ClassDescriptor.FieldType.tpSByte: 
                        buf.extend(offs + 1);
                        if (elem != null)
                        {
                            if (elem.isIntValue())
                            {
                                buf.arr[offs] = (byte) elem.IntValue;
                            }
                            else if (elem.isRealValue())
                            {
                                buf.arr[offs] = (byte) elem.RealValue;
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                buf.arr[offs] = (byte) (elem.IntValue != 0?1:0);
                            }
                            else if (elem.isRealValue())
                            {
                                buf.arr[offs] = (byte) (elem.RealValue != 0.0?1:0);
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack2(buf.arr, offs, (short) elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack2(buf.arr, offs, (short) elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack4(buf.arr, offs, (int) elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int) elem.RealValue);
                            }
                            else if (elem.isStringValue()) 
                            {
                                try 
                                {
                                    Bytes.pack4(buf.arr, offs, (int)Enum.Parse(f.FieldType, elem.StringValue));
                                } 
                                catch (ArgumentException x)
                                {
                                    throwException("Invalid enum value");
                                }
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack4(buf.arr, offs, (int) elem.IntValue);
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack4(buf.arr, offs, (int) elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack8(buf.arr, offs, (long) elem.RealValue);
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes((float) elem.IntValue), 0));
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes((float) elem.RealValue), 0));
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                Bytes.pack8(buf.arr, offs, BitConverter.DoubleToInt64Bits((double) elem.IntValue));
                            }
                            else if (elem.isRealValue())
                            {
                                Bytes.pack8(buf.arr, offs, BitConverter.DoubleToInt64Bits((double) elem.RealValue));
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
                            }
                        }
                        offs += 8;
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
                                Bytes.pack8(buf.arr, offs, - 1);
                            }
                            else if (elem.isStringValue())
                            {
                                try 
                                { 
                                    Bytes.pack8(buf.arr, offs, DateTime.Parse(elem.StringValue).Ticks);
                                } 
                                catch (FormatException x) 
                                {
                                    throwException("Invalid date");
                                }
                            }
                            else
                            {
                                throwException("Conversion for field " + f.Name + " is not possible");
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
                                throwException("Conversion for field " + f.Name + " is not possible");
                            }
                            if (val != null)
                            {
                                int len = val.Length;
                                buf.extend(offs + 4 + len * 2);
                                Bytes.pack4(buf.arr, offs, len);
                                offs += 4;
                                for (int j = 0; j < len; j++)
                                {
                                    Bytes.pack2(buf.arr, offs, (short) val[j]);
                                    offs += 2;
                                }
                                continue;
                            }
                        }
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, - 1);
                        offs += 4;
                        continue;
					
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
                        offs = packObject(elem, storage.getClassDescriptor(f.FieldType), offs, buf);
                        continue;
					
                    case ClassDescriptor.FieldType.tpRaw: 
                    case ClassDescriptor.FieldType.tpArrayOfByte: 
                    case ClassDescriptor.FieldType.tpArrayOfSByte: 
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else if (elem.isStringValue())
                        {
                            System.String hexStr = elem.StringValue;
                            int len = hexStr.Length;
                            buf.extend(offs + 4 + len / 2);
                            Bytes.pack4(buf.arr, offs, len / 2);
                            offs += 4;
                            for (int j = 0; j < len; j += 2)
                            {
                                buf.arr[offs++] = (byte) ((getHexValue(hexStr[j]) << 4) | getHexValue(hexStr[j + 1]));
                            }
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    buf.arr[offs] = (byte) item.IntValue;
                                }
                                else if (item.isRealValue())
                                {
                                    buf.arr[offs] = (byte) item.RealValue;
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
                                }
                                item = item.NextSibling;
                                offs += 1;
                            }
                        }
                        continue;
					
                    case ClassDescriptor.FieldType.tpArrayOfBoolean: 
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    buf.arr[offs] = (byte) (item.IntValue != 0?1:0);
                                }
                                else if (item.isRealValue())
                                {
                                    buf.arr[offs] = (byte) (item.RealValue != 0.0?1:0);
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 2);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack2(buf.arr, offs, (short) item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack2(buf.arr, offs, (short) item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            Type elemType = f.FieldType.GetElementType();
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int) item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int) item.RealValue);
                                }
                                else if (item.isStringValue()) 
                                {
                                    try 
                                    {
                                        Bytes.pack4(buf.arr, offs, (int)Enum.Parse(elemType, item.StringValue));
                                    } 
                                    catch (ArgumentException x)
                                    {
                                        throwException("Invalid enum value");
                                    }
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int) item.IntValue);
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack4(buf.arr, offs, (int) item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
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
                                    Bytes.pack8(buf.arr, offs, (long) item.RealValue);
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                   Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes((float)item.IntValue), 0));
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack4(buf.arr, offs, BitConverter.ToInt32(BitConverter.GetBytes((float)item.RealValue), 0));
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isIntValue())
                                {
                                    Bytes.pack8(buf.arr, offs,  BitConverter.DoubleToInt64Bits((double) item.IntValue));
                                }
                                else if (item.isRealValue())
                                {
                                    Bytes.pack8(buf.arr, offs,  BitConverter.DoubleToInt64Bits(item.RealValue));
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 8);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isNullValue())
                                {
                                    Bytes.pack8(buf.arr, offs, - 1);
                                }
                                else if (item.isStringValue())
                                {
                                    try 
                                    { 
                                        Bytes.pack8(buf.arr, offs, DateTime.Parse(item.StringValue).Ticks);
                                    }
                                    catch (FormatException x)
                                    {
                                        throwException("Conversion for field " + f.Name + " is not possible");
                                    }
                                }
                                item = item.NextSibling;
                                offs += 8;
                            }
                        }
                        continue;
					
                    case ClassDescriptor.FieldType.tpArrayOfString: 
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 4);
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
                                else if (elem.isNullValue())
                                {
                                    val = null;
                                }
                                else
                                {
                                    throwException("Conversion for field " + f.Name + " is not possible");
                                }
                                if (val == null)
                                {
                                    buf.extend(offs + 4);
                                    Bytes.pack4(buf.arr, offs, - 1);
                                    offs += 4;
                                }
                                else
                                {
                                    int strlen = val.Length;
                                    buf.extend(offs + 4 + len * 2);
                                    Bytes.pack4(buf.arr, offs, len);
                                    offs += 4;
                                    for (int j = 0; j < strlen; j++)
                                    {
                                        Bytes.pack2(buf.arr, offs, (short) val[j]);
                                        offs += 2;
                                    }
                                }
                                item = item.NextSibling;
                            }
                        }
                        continue;
					
                    case ClassDescriptor.FieldType.tpArrayOfObject: 
                    case ClassDescriptor.FieldType.tpLink: 
                        if (elem == null || elem.isNullValue())
                        {
                            buf.extend(offs + 4);
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            buf.extend(offs + 4 + len * 4);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                XMLElement ref_Renamed = item.getSibling("ref");
                                if (ref_Renamed == null)
                                {
                                    throwException("<ref> element expected");
                                }
                                int oid = mapId(getIntAttribute(ref_Renamed, "id"));
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            ClassDescriptor elemDesc = storage.getClassDescriptor(f.FieldType);
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
                            Bytes.pack4(buf.arr, offs, - 1);
                            offs += 4;
                        }
                        else
                        {
                            XMLElement item = elem.getSibling("array-element");
                            int len = (item == null)?0:item.Counter;
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            while (--len >= 0)
                            {
                                if (item.isNullValue()) 
                                {
                                    buf.extend(offs + 4);
                                    Bytes.pack4(buf.arr, offs, -1);
                                    offs += 4;
                                } 
                                else if (item.isStringValue()) 
                                {
                                    String hexStr = item.StringValue;
                                    int strlen = hexStr.Length;
                                    buf.extend(offs + 4 + strlen/2);
                                    Bytes.pack4(buf.arr, offs, strlen/2);
                                    offs += 4;
                                    for (int j = 0; j < strlen; j += 2) 
                                    { 
                                        buf.arr[offs++] = (byte)((getHexValue(hexStr[j]) << 4) | getHexValue(hexStr[j+1]));
                                    }
                                } 
                                else 
                                { 
                                    throwException("String with hexadecimal dump expected");
                                }
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
		
        internal void  throwException(System.String message)
        {
            throw new XMLImportException(scanner.Line, scanner.Column, message);
        }
		
        internal StorageImpl storage;
        internal XMLScanner  scanner;
        internal Hashtable   classMap;
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
            };
			
            internal System.IO.StreamReader reader;
            internal int    line;
            internal int    column;
            internal char[] sconst;
            internal long   iconst;
            internal double fconst;
            internal int    slen;
            internal String ident;
            internal int    size;
            internal int    ungetChar;
            internal bool   hasUngetChar;
			
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
                    column += (column + 8) & ~ 7;
                }
                else
                {
                    column += 1;
                }
                return ch;
            }
			
            internal void  unget(int ch)
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
                                        throw new XMLImportException(line, column, "Bad XML file format");
                                    }
                                }
                                if ((ch = get()) != '>')
                                {
                                    throw new XMLImportException(line, column, "Bad XML file format");
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
                                throw new XMLImportException(line, column, "Bad XML file format");
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
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                else if (ch == '&')
                                {
                                    switch (get())
                                    {
                                        case 'a': 
                                            if (get() != 'm' || get() != 'p' || get() != ';')
                                            {
                                                throw new XMLImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '&';
                                            break;
										
                                        case 'l': 
                                            if (get() != 't' || get() != ';')
                                            {
                                                throw new XMLImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '<';
                                            break;
										
                                        case 'g': 
                                            if (get() != 't' || get() != ';')
                                            {
                                                throw new XMLImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '>';
                                            break;
										
                                        case 'q': 
                                            if (get() != 'u' || get() != 'o' || get() != 't' || get() != ';')
                                            {
                                                throw new XMLImportException(line, column, "Bad XML file format");
                                            }
                                            ch = '"';
                                            break;
										
                                        default: 
                                            throw new XMLImportException(line, column, "Bad XML file format");
										
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
                                    Array.Copy(sconst, 0, newBuf, 0, slen);
                                    sconst = newBuf;
                                }
                                sconst[i++] = (char) ch;
                            }
						
                        case '-': case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': 
                            i = 0;
                            floatingPoint = false;
                            while (true)
                            {
                                if (!System.Char.IsDigit((char) ch) && ch != '-' && ch != '+' && ch != '.' && ch != 'E')
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
                                            iconst = System.Int64.Parse(new String(sconst, 0, i));
                                            return Token.ICONST;
                                        }
                                    }
                                    catch (System.FormatException x)
                                    {
                                        throw new XMLImportException(line, column, "Bad XML file format");
                                    }
                                }
                                if (i == size)
                                {
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                sconst[i++] = (char) ch;
                                if (ch == '.')
                                {
                                    floatingPoint = true;
                                }
                                ch = get();
                            }
                            goto default;
						
                        default: 
                            i = 0;
                            while (System.Char.IsLetterOrDigit((char) ch) || ch == '+' || ch == '$' || ch == '-' || ch == ':' || ch == '_' || ch == '.')
                            {
                                if (i == size)
                                {
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                sconst[i++] = (char) ch;
                                ch = get();
                            }
                            unget(ch);
                            if (i == 0)
                            {
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                            ident = new String(sconst, 0, i);
                            return Token.IDENT;
                    }
                }
            }
        }
    }
}