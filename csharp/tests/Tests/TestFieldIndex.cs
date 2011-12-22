namespace Volante
{
    using System;

    public class TestFieldIndex : ITest
    {
        public class RecordAuto : Persistent
        {
            public int IntAuto;
            public long LongAuto { get; set; }
            public bool BoolNoAuto;
        }

        public class Root : Persistent
        {
            public IFieldIndex<bool, RecordFullWithProperty> idxBool;
            public IFieldIndex<byte, RecordFullWithProperty> idxByte;
            public IFieldIndex<sbyte, RecordFullWithProperty> idxSByte;
            public IFieldIndex<short, RecordFullWithProperty> idxShort;
            public IFieldIndex<ushort, RecordFullWithProperty> idxUShort;
            public IFieldIndex<int, RecordFullWithProperty> idxInt;
            public IFieldIndex<uint, RecordFullWithProperty> idxUInt;
            public IFieldIndex<long, RecordFullWithProperty> idxLong;
            public IFieldIndex<long, RecordFullWithProperty> idxLongProp;
            public IFieldIndex<ulong, RecordFullWithProperty> idxULong;
            // TODO: Btree.allocateRootPage() doesn't support tpChar even though 
            // FieldIndex does support it as a key and OldBtree supports it.
            //public IFieldIndex<char, RecordFullWithProperty> idxChar;
            public IFieldIndex<float, RecordFullWithProperty> idxFloat;
            public IFieldIndex<double, RecordFullWithProperty> idxDouble;
            public IFieldIndex<DateTime, RecordFullWithProperty> idxDate;
            public IFieldIndex<decimal, RecordFullWithProperty> idxDecimal;
            public IFieldIndex<Guid, RecordFullWithProperty> idxGuid;
            public IFieldIndex<string, RecordFullWithProperty> idxString;
            // TODO: Btree.allocateRootPage() doesn't support tpEnum even though 
            // FieldIndex does support it as a key and OldBtree supports it.
            //public IFieldIndex<RecordFullWithPropertyEnum, RecordFullWithProperty> idxEnum;
            // TODO: OldBtree doesn't support oid as an index
            //public IFieldIndex<object, RecordFullWithProperty> idxObject;
            public IFieldIndex<int, RecordFullWithProperty> idxOid;

            public IFieldIndex<int, RecordAuto> idxIntAuto;
            public IFieldIndex<long, RecordAuto> idxLongAuto;
        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;

            IDatabase db = config.GetDatabase();
            Tests.Assert(db.Root == null);
            Root root = new Root();
            Tests.AssertDatabaseException(() =>
                { root.idxBool = db.CreateFieldIndex<bool, RecordFullWithProperty>("NonExistent", IndexType.NonUnique); },
                DatabaseException.ErrorCode.INDEXED_FIELD_NOT_FOUND);

            Tests.AssertDatabaseException(() =>
                { root.idxBool = db.CreateFieldIndex<bool, RecordFullWithProperty>("CharVal", IndexType.NonUnique); },
                DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);

            root.idxBool = db.CreateFieldIndex<bool, RecordFullWithProperty>("BoolVal", IndexType.NonUnique);
            root.idxByte = db.CreateFieldIndex<byte, RecordFullWithProperty>("ByteVal", IndexType.NonUnique);
            root.idxSByte = db.CreateFieldIndex<sbyte, RecordFullWithProperty>("SByteVal", IndexType.NonUnique);
            root.idxShort = db.CreateFieldIndex<short, RecordFullWithProperty>("Int16Val", IndexType.NonUnique);
            root.idxUShort = db.CreateFieldIndex<ushort, RecordFullWithProperty>("UInt16Val", IndexType.NonUnique);
            root.idxInt = db.CreateFieldIndex<int, RecordFullWithProperty>("Int32Val", IndexType.NonUnique);
            root.idxUInt = db.CreateFieldIndex<uint, RecordFullWithProperty>("UInt32Val", IndexType.NonUnique);
            root.idxLong = db.CreateFieldIndex<long, RecordFullWithProperty>("Int64Val", IndexType.Unique);
            root.idxLongProp = db.CreateFieldIndex<long, RecordFullWithProperty>("Int64Prop", IndexType.Unique);
            root.idxULong = db.CreateFieldIndex<ulong, RecordFullWithProperty>("UInt64Val", IndexType.NonUnique);
            //root.idxChar = db.CreateFieldIndex<char, RecordFullWithProperty>("CharVal", IndexType.NonUnique);
            root.idxFloat = db.CreateFieldIndex<float, RecordFullWithProperty>("FloatVal", IndexType.NonUnique);
            root.idxDouble = db.CreateFieldIndex<double, RecordFullWithProperty>("DoubleVal", IndexType.NonUnique);
            root.idxDate = db.CreateFieldIndex<DateTime, RecordFullWithProperty>("DateTimeVal", IndexType.NonUnique);
            root.idxDecimal = db.CreateFieldIndex<Decimal, RecordFullWithProperty>("DecimalVal", IndexType.NonUnique);
            root.idxGuid = db.CreateFieldIndex<Guid, RecordFullWithProperty>("GuidVal", IndexType.NonUnique);
            root.idxString = db.CreateFieldIndex<String, RecordFullWithProperty>("StrVal", IndexType.NonUnique);
            //root.idxEnum = db.CreateFieldIndex<RecordFullWithPropertyEnum, RecordFullWithProperty>("EnumVal", IndexType.NonUnique);
            //root.idxObject = db.CreateFieldIndex<object, RecordFullWithProperty>("ObjectVal", IndexType.NonUnique);
            root.idxOid = db.CreateFieldIndex<int, RecordFullWithProperty>("Oid", IndexType.NonUnique);

            root.idxIntAuto = db.CreateFieldIndex<int, RecordAuto>("IntAuto", IndexType.Unique);
            root.idxLongAuto = db.CreateFieldIndex<long, RecordAuto>("LongAuto", IndexType.Unique);
            db.Root = root;

            Tests.Assert(root.idxString.IndexedClass == typeof(RecordFullWithProperty));
            Tests.Assert(root.idxString.KeyField.Name == "StrVal");

            int i = 0;
            RecordFullWithProperty rfFirst = null;
            RecordAuto raFirst = null;
            long firstKey = 0;
            foreach (long key in Tests.KeySeq(count))
            {
                var r = new RecordFullWithProperty(key);
                root.idxBool.Put(r);
                root.idxByte.Put(r);
                root.idxSByte.Put(r);
                root.idxShort.Put(r);
                root.idxUShort.Put(r);
                root.idxInt.Put(r);
                root.idxUInt.Put(r);
                root.idxLong.Put(r);
                root.idxLongProp.Put(r);
                root.idxULong.Put(r);
                //root.idxChar.Put(r);
                root.idxFloat.Put(r);
                root.idxDouble.Put(r);
                root.idxDate.Put(r);
                root.idxDecimal.Put(r);
                root.idxGuid.Put(r);
                root.idxString.Put(r);
                //root.idxEnum.Put(r);
                //root.idxObject.Put(r);
                root.idxOid.Put(r);

                var ra = new RecordAuto();
                root.idxIntAuto.Append(ra);
                root.idxLongAuto.Append(ra);
                Tests.Assert(ra.IntAuto == i);
                Tests.Assert(ra.LongAuto == i);
                i++;
                if (null == rfFirst)
                {
                    rfFirst = r;
                    raFirst = ra;
                    firstKey = key;
                }
            }
            db.Commit();
            var r2 = new RecordFullWithProperty(firstKey);
            // Contains for unique index
            Tests.Assert(root.idxLong.Contains(rfFirst));
            Tests.Assert(!root.idxLong.Contains(r2));

            // Contains() for non-unique index
            Tests.Assert(root.idxInt.Contains(rfFirst));
            Tests.Assert(!root.idxInt.Contains(r2));

            Tests.Assert(false == root.idxLongProp.Put(r2));
            root.idxLongProp.Set(r2);

            Tests.AssertDatabaseException(() =>
                { root.idxString.Append(rfFirst); },
                DatabaseException.ErrorCode.UNSUPPORTED_INDEX_TYPE);

            Tests.Assert(root.idxBool.Remove(rfFirst));
            Tests.Assert(root.idxByte.Remove(rfFirst));
            Tests.Assert(root.idxSByte.Remove(rfFirst));
            Tests.Assert(root.idxShort.Remove(rfFirst));
            Tests.Assert(root.idxUShort.Remove(rfFirst));
            Tests.Assert(root.idxInt.Remove(rfFirst));
            Tests.Assert(root.idxUInt.Remove(rfFirst));
            Tests.Assert(root.idxLong.Remove(rfFirst));
            Tests.Assert(root.idxULong.Remove(rfFirst));
            Tests.Assert(root.idxFloat.Remove(rfFirst));
            Tests.Assert(root.idxDouble.Remove(rfFirst));
            Tests.Assert(root.idxDate.Remove(rfFirst));
            Tests.Assert(root.idxDecimal.Remove(rfFirst));
            Tests.Assert(root.idxGuid.Remove(rfFirst));
            Tests.Assert(root.idxString.Remove(rfFirst));
            db.Commit();
            Tests.Assert(!root.idxBool.Remove(rfFirst));
            Tests.Assert(!root.idxByte.Remove(rfFirst));
            Tests.Assert(!root.idxSByte.Remove(rfFirst));
            Tests.Assert(!root.idxShort.Remove(rfFirst));
            Tests.Assert(!root.idxUShort.Remove(rfFirst));
            Tests.Assert(!root.idxInt.Remove(rfFirst));
            Tests.Assert(!root.idxUInt.Remove(rfFirst));
            Tests.Assert(!root.idxLong.Remove(rfFirst));
            Tests.Assert(!root.idxLongProp.Remove(rfFirst));
            Tests.Assert(!root.idxULong.Remove(rfFirst));
            Tests.Assert(!root.idxFloat.Remove(rfFirst));
            Tests.Assert(!root.idxDouble.Remove(rfFirst));
            Tests.Assert(!root.idxDate.Remove(rfFirst));
            Tests.Assert(!root.idxDecimal.Remove(rfFirst));
            Tests.Assert(!root.idxGuid.Remove(rfFirst));
            Tests.Assert(!root.idxString.Remove(rfFirst));
            db.Commit();
            var e = root.idxLong.GetEnumerator();
            Tests.Assert(e.MoveNext());
            r2 = e.Current;
            Tests.Assert(root.idxLongProp.Remove(r2));
            db.Commit();
            Tests.Assert(!root.idxLongProp.Remove(r2));
        }
    }
}
