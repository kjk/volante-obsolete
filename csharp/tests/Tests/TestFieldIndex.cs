namespace Volante
{
    using System;

    public class TestFieldIndex : ITest
    {
        public class RecordAuto : Persistent
        {
            public int IntAuto;
            public long LongAuto;
            public bool BoolNoAuto;
        }


        public class Root : Persistent
        {
            public IFieldIndex<bool, RecordFull> idxBool;
            public IFieldIndex<byte, RecordFull> idxByte;
            public IFieldIndex<sbyte, RecordFull> idxSByte;
            public IFieldIndex<short, RecordFull> idxShort;
            public IFieldIndex<ushort, RecordFull> idxUShort;
            public IFieldIndex<int, RecordFull> idxInt;
            public IFieldIndex<uint, RecordFull> idxUInt;
            public IFieldIndex<long, RecordFull> idxLong;
            public IFieldIndex<long, RecordFull> idxLongProp;
            public IFieldIndex<ulong, RecordFull> idxULong;
            // TODO: Btree.allocateRootPage() doesn't support tpChar even though 
            // FieldIndex does support it as a key and OldBtree supports it.
            //public IFieldIndex<char, RecordFull> idxChar;
            public IFieldIndex<float, RecordFull> idxFloat;
            public IFieldIndex<double, RecordFull> idxDouble;
            public IFieldIndex<DateTime, RecordFull> idxDate;
            public IFieldIndex<decimal, RecordFull> idxDecimal;
            public IFieldIndex<Guid, RecordFull> idxGuid;
            public IFieldIndex<string, RecordFull> idxString;
            // TODO: Btree.allocateRootPage() doesn't support tpEnum even though 
            // FieldIndex does support it as a key and OldBtree supports it.
            //public IFieldIndex<RecordFullEnum, RecordFull> idxEnum;
            // TODO: OldBtree doesn't support oid as an index
            //public IFieldIndex<object, RecordFull> idxObject;
            public IFieldIndex<int, RecordFull> idxOid;

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
                { root.idxBool = db.CreateFieldIndex<bool, RecordFull>("NonExistent", IndexType.NonUnique); },
                DatabaseException.ErrorCode.INDEXED_FIELD_NOT_FOUND);

            Tests.AssertDatabaseException(() =>
                { root.idxBool = db.CreateFieldIndex<bool, RecordFull>("CharVal", IndexType.NonUnique); },
                DatabaseException.ErrorCode.INCOMPATIBLE_KEY_TYPE);

            root.idxBool = db.CreateFieldIndex<bool, RecordFull>("BoolVal", IndexType.NonUnique);
            root.idxByte = db.CreateFieldIndex<byte, RecordFull>("ByteVal", IndexType.NonUnique);
            root.idxSByte = db.CreateFieldIndex<sbyte, RecordFull>("SByteVal", IndexType.NonUnique);
            root.idxShort = db.CreateFieldIndex<short, RecordFull>("Int16Val", IndexType.NonUnique);
            root.idxUShort = db.CreateFieldIndex<ushort, RecordFull>("UInt16Val", IndexType.NonUnique);
            root.idxInt = db.CreateFieldIndex<int, RecordFull>("Int32Val", IndexType.NonUnique);
            root.idxUInt = db.CreateFieldIndex<uint, RecordFull>("UInt32Val", IndexType.NonUnique);
            root.idxLong = db.CreateFieldIndex<long, RecordFull>("Int64Val", IndexType.Unique);
            root.idxLongProp = db.CreateFieldIndex<long, RecordFull>("Int64Prop", IndexType.Unique);
            root.idxULong = db.CreateFieldIndex<ulong, RecordFull>("UInt64Val", IndexType.NonUnique);
            //root.idxChar = db.CreateFieldIndex<char, RecordFull>("CharVal", IndexType.NonUnique);
            root.idxFloat = db.CreateFieldIndex<float, RecordFull>("FloatVal", IndexType.NonUnique);
            root.idxDouble = db.CreateFieldIndex<double, RecordFull>("DoubleVal", IndexType.NonUnique);
            root.idxDate = db.CreateFieldIndex<DateTime, RecordFull>("DateTimeVal", IndexType.NonUnique);
            root.idxDecimal = db.CreateFieldIndex<Decimal, RecordFull>("DecimalVal", IndexType.NonUnique);
            root.idxGuid = db.CreateFieldIndex<Guid, RecordFull>("GuidVal", IndexType.NonUnique);
            root.idxString = db.CreateFieldIndex<String, RecordFull>("StrVal", IndexType.NonUnique);
            //root.idxEnum = db.CreateFieldIndex<RecordFullEnum, RecordFull>("EnumVal", IndexType.NonUnique);
            //root.idxObject = db.CreateFieldIndex<object, RecordFull>("ObjectVal", IndexType.NonUnique);
            root.idxOid = db.CreateFieldIndex<int, RecordFull>("Oid", IndexType.NonUnique);

            root.idxIntAuto = db.CreateFieldIndex<int, RecordAuto>("IntAuto", IndexType.Unique);
            root.idxLongAuto = db.CreateFieldIndex<long, RecordAuto>("LongAuto", IndexType.Unique);
            db.Root = root;

            Tests.Assert(root.idxString.IndexedClass == typeof(RecordFull));
            Tests.Assert(root.idxString.KeyField.Name == "StrVal");

            int i = 0;
            RecordFull rfFirst = null;
            RecordAuto raFirst = null;
            long firstKey = 0;
            foreach (long key in Tests.KeySeq(count))
            {
                var r = new RecordFull(key);
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
            var r2 = new RecordFull(firstKey);
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

            Tests.Assert(root.idxLong.Remove(rfFirst));
            db.Commit();
            Tests.Assert(!root.idxLong.Remove(rfFirst));
        }
    }
}
