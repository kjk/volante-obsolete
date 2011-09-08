#if WITH_OLD_BTREE
namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using Volante;

    internal class OldBtreePage
    {
        internal const int firstKeyOffs = 4;
        internal const int keySpace = Page.pageSize - firstKeyOffs;
        internal const int strKeySize = 8;
        internal const int maxItems = keySpace / 4;

        internal static int getnItems(Page pg)
        {
            return Bytes.unpack2(pg.data, 0);
        }
        internal static int getSize(Page pg)
        {
            return Bytes.unpack2(pg.data, 2);
        }
        internal static int getKeyStrOid(Page pg, int index)
        {
            return Bytes.unpack4(pg.data, firstKeyOffs + index * 8);
        }
        internal static int getKeyStrSize(Page pg, int index)
        {
            return Bytes.unpack2(pg.data, firstKeyOffs + index * 8 + 4);
        }
        internal static int getKeyStrOffs(Page pg, int index)
        {
            return Bytes.unpack2(pg.data, firstKeyOffs + index * 8 + 6);
        }

        internal static int getReference(Page pg, int index)
        {
            return Bytes.unpack4(pg.data, firstKeyOffs + index * 4);
        }

        internal static void setnItems(Page pg, int nItems)
        {
            Bytes.pack2(pg.data, 0, (short)nItems);
        }
        internal static void setSize(Page pg, int size)
        {
            Bytes.pack2(pg.data, 2, (short)size);
        }
        internal static void setKeyStrOid(Page pg, int index, int oid)
        {
            Bytes.pack4(pg.data, firstKeyOffs + index * 8, oid);
        }
        internal static void setKeyStrSize(Page pg, int index, int size)
        {
            Bytes.pack2(pg.data, firstKeyOffs + index * 8 + 4, (short)size);
        }
        internal static void setKeyStrOffs(Page pg, int index, int offs)
        {
            Bytes.pack2(pg.data, firstKeyOffs + index * 8 + 6, (short)offs);
        }
        internal static void setKeyStrChars(Page pg, int offs, char[] str)
        {
            int len = str.Length;
            for (int i = 0; i < len; i++)
            {
                Bytes.pack2(pg.data, firstKeyOffs + offs, (short)str[i]);
                offs += 2;
            }
        }
        internal static void setKeyBytes(Page pg, int offs, byte[] bytes)
        {
            Array.Copy(bytes, 0, pg.data, firstKeyOffs + offs, bytes.Length);
        }

        internal static void setReference(Page pg, int index, int oid)
        {
            Bytes.pack4(pg.data, firstKeyOffs + index * 4, oid);
        }

        internal static int compare(Key key, Page pg, int i)
        {
            long i8;
            ulong u8;
            int i4;
            uint u4;
            float r4;
            double r8;
            switch (key.type)
            {
                case ClassDescriptor.FieldType.tpSByte:
                    return (sbyte)key.ival - (sbyte)pg.data[OldBtreePage.firstKeyOffs + i];

                case ClassDescriptor.FieldType.tpBoolean:
                case ClassDescriptor.FieldType.tpByte:
                    return (byte)key.ival - pg.data[OldBtreePage.firstKeyOffs + i];

                case ClassDescriptor.FieldType.tpShort:
                    return (short)key.ival - Bytes.unpack2(pg.data, OldBtreePage.firstKeyOffs + i * 2);
                case ClassDescriptor.FieldType.tpUShort:
                    return (ushort)key.ival - (ushort)Bytes.unpack2(pg.data, OldBtreePage.firstKeyOffs + i * 2);

                case ClassDescriptor.FieldType.tpChar:
                    return (char)key.ival - (char)Bytes.unpack2(pg.data, OldBtreePage.firstKeyOffs + i * 2);

                case ClassDescriptor.FieldType.tpObject:
                case ClassDescriptor.FieldType.tpUInt:
                case ClassDescriptor.FieldType.tpOid:
                case ClassDescriptor.FieldType.tpEnum:
                    u4 = (uint)Bytes.unpack4(pg.data, OldBtreePage.firstKeyOffs + i * 4);
                    return (uint)key.ival < u4 ? -1 : (uint)key.ival == u4 ? 0 : 1;

                case ClassDescriptor.FieldType.tpInt:
                    i4 = Bytes.unpack4(pg.data, OldBtreePage.firstKeyOffs + i * 4);
                    return key.ival < i4 ? -1 : key.ival == i4 ? 0 : 1;

                case ClassDescriptor.FieldType.tpLong:
                    i8 = Bytes.unpack8(pg.data, OldBtreePage.firstKeyOffs + i * 8);
                    return key.lval < i8 ? -1 : key.lval == i8 ? 0 : 1;

                case ClassDescriptor.FieldType.tpDate:
                case ClassDescriptor.FieldType.tpULong:
                    u8 = (ulong)Bytes.unpack8(pg.data, OldBtreePage.firstKeyOffs + i * 8);
                    return (ulong)key.lval < u8 ? -1 : (ulong)key.lval == u8 ? 0 : 1;

                case ClassDescriptor.FieldType.tpFloat:
                    r4 = Bytes.unpackF4(pg.data, OldBtreePage.firstKeyOffs + i * 4);
                    return key.dval < r4 ? -1 : key.dval == r4 ? 0 : 1;

                case ClassDescriptor.FieldType.tpDouble:
                    r8 = Bytes.unpackF8(pg.data, OldBtreePage.firstKeyOffs + i * 8);
                    return key.dval < r8 ? -1 : key.dval == r8 ? 0 : 1;

                case ClassDescriptor.FieldType.tpDecimal:
                    return key.dec.CompareTo(Bytes.unpackDecimal(pg.data, OldBtreePage.firstKeyOffs + i * 16));

                case ClassDescriptor.FieldType.tpGuid:
                    return key.guid.CompareTo(Bytes.unpackGuid(pg.data, OldBtreePage.firstKeyOffs + i * 16));
            }
            Debug.Assert(false, "Invalid type");
            return 0;
        }

        internal static int compareStr(Key key, Page pg, int i)
        {
            char[] chars = null;
            string s = key.oval as string;
            if (s != null)
                chars = s.ToCharArray();
            else
                chars = (char[])key.oval;
            int alen = chars.Length;
            int blen = OldBtreePage.getKeyStrSize(pg, i);
            int minlen = alen < blen ? alen : blen;
            int offs = OldBtreePage.getKeyStrOffs(pg, i) + OldBtreePage.firstKeyOffs;
            byte[] b = pg.data;
            for (int j = 0; j < minlen; j++)
            {
                int diff = chars[j] - (char)Bytes.unpack2(b, offs);
                if (diff != 0)
                    return diff;
                offs += 2;
            }
            return alen - blen;
        }

        internal static bool find(DatabaseImpl db, int pageId, Key firstKey, Key lastKey, OldBtree tree, int height, ArrayList result)
        {
            Page pg = db.getPage(pageId);
            int l = 0, n = getnItems(pg), r = n;
            int oid;
            height -= 1;
            try
            {
                if (tree.FieldType == ClassDescriptor.FieldType.tpString)
                {
                    if (firstKey != null)
                    {
                        while (l < r)
                        {
                            int i = (l + r) >> 1;
                            if (compareStr(firstKey, pg, i) >= firstKey.inclusion)
                                l = i + 1;
                            else
                                r = i;
                        }
                        Debug.Assert(r == l);
                    }
                    if (lastKey != null)
                    {
                        if (height == 0)
                        {
                            while (l < n)
                            {
                                if (-compareStr(lastKey, pg, l) >= lastKey.inclusion)
                                    return false;
                                oid = getKeyStrOid(pg, l);
                                result.Add(db.lookupObject(oid, null));
                                l += 1;
                            }
                        }
                        else
                        {
                            do
                            {
                                if (!find(db, getKeyStrOid(pg, l), firstKey, lastKey, tree, height, result))
                                    return false;
                                if (l == n)
                                    return true;
                            }
                            while (compareStr(lastKey, pg, l++) >= 0);
                            return false;
                        }
                    }
                    else
                    {
                        if (height == 0)
                        {
                            while (l < n)
                            {
                                oid = getKeyStrOid(pg, l);
                                result.Add(db.lookupObject(oid, null));
                                l += 1;
                            }
                        }
                        else
                        {
                            do
                            {
                                if (!find(db, getKeyStrOid(pg, l), firstKey, lastKey, tree, height, result))
                                    return false;
                            }
                            while (++l <= n);
                        }
                    }
                }
                else if (tree.FieldType == ClassDescriptor.FieldType.tpArrayOfByte)
                {
                    if (firstKey != null)
                    {
                        while (l < r)
                        {
                            int i = (l + r) >> 1;
                            if (tree.compareByteArrays(firstKey, pg, i) >= firstKey.inclusion)
                                l = i + 1;
                            else
                                r = i;
                        }
                        Debug.Assert(r == l);
                    }
                    if (lastKey != null)
                    {
                        if (height == 0)
                        {
                            while (l < n)
                            {
                                if (-tree.compareByteArrays(lastKey, pg, l) >= lastKey.inclusion)
                                    return false;
                                oid = getKeyStrOid(pg, l);
                                result.Add(db.lookupObject(oid, null));
                                l += 1;
                            }
                        }
                        else
                        {
                            do
                            {
                                if (!find(db, getKeyStrOid(pg, l), firstKey, lastKey, tree, height, result))
                                    return false;
                                if (l == n)
                                    return true;
                            }
                            while (tree.compareByteArrays(lastKey, pg, l++) >= 0);
                            return false;
                        }
                    }
                    else
                    {
                        if (height == 0)
                        {
                            while (l < n)
                            {
                                oid = getKeyStrOid(pg, l);
                                result.Add(db.lookupObject(oid, null));
                                l += 1;
                            }
                        }
                        else
                        {
                            do
                            {
                                if (!find(db, getKeyStrOid(pg, l), firstKey, lastKey, tree, height, result))
                                    return false;
                            }
                            while (++l <= n);
                        }
                    }
                }
                else
                {
                    if (firstKey != null)
                    {
                        while (l < r)
                        {
                            int i = (l + r) >> 1;
                            if (compare(firstKey, pg, i) >= firstKey.inclusion)
                                l = i + 1;
                            else
                                r = i;
                        }
                        Debug.Assert(r == l);
                    }
                    if (lastKey != null)
                    {
                        if (height == 0)
                        {
                            while (l < n)
                            {
                                if (-compare(lastKey, pg, l) >= lastKey.inclusion)
                                    return false;
                                oid = getReference(pg, maxItems - 1 - l);
                                result.Add(db.lookupObject(oid, null));
                                l += 1;
                            }
                            return true;
                        }
                        else
                        {
                            do
                            {
                                if (!find(db, getReference(pg, maxItems - 1 - l), firstKey, lastKey, tree, height, result))
                                    return false;

                                if (l == n)
                                    return true;
                            }
                            while (compare(lastKey, pg, l++) >= 0);
                            return false;
                        }
                    }
                    if (height == 0)
                    {
                        while (l < n)
                        {
                            oid = getReference(pg, maxItems - 1 - l);
                            result.Add(db.lookupObject(oid, null));
                            l += 1;
                        }
                    }
                    else
                    {
                        do
                        {
                            if (!find(db, getReference(pg, maxItems - 1 - l), firstKey, lastKey, tree, height, result))
                                return false;
                        }
                        while (++l <= n);
                    }
                }
            }
            finally
            {
                db.pool.unfix(pg);
            }
            return true;
        }

        static int comparePrefix(string key, Page pg, int i)
        {
            int alen = key.Length;
            int blen = OldBtreePage.getKeyStrSize(pg, i);
            int minlen = alen < blen ? alen : blen;
            int offs = OldBtreePage.getKeyStrOffs(pg, i) + OldBtreePage.firstKeyOffs;
            byte[] b = pg.data;
            for (int j = 0; j < minlen; j++)
            {
                char c = (char)Bytes.unpack2(b, offs);
                int diff = key[j] - c;
                if (diff != 0)
                    return diff;
                offs += 2;
            }
            return minlen - alen;
        }

        internal static bool prefixSearch(DatabaseImpl db, int pageId, string key,
                                    int height, ArrayList result)
        {
            Page pg = db.getPage(pageId);
            int l = 0, n = getnItems(pg), r = n;
            int oid;
            height -= 1;
            try
            {
                while (l < r)
                {
                    int i = (l + r) >> 1;
                    if (comparePrefix(key, pg, i) > 0)
                        l = i + 1;
                    else
                        r = i;
                }
                Debug.Assert(r == l);
                if (height == 0)
                {
                    while (l < n)
                    {
                        if (comparePrefix(key, pg, l) < 0)
                            return false;

                        oid = getKeyStrOid(pg, l);
                        result.Add(db.lookupObject(oid, null));
                        l += 1;
                    }
                }
                else
                {
                    do
                    {
                        if (!prefixSearch(db, getKeyStrOid(pg, l), key, height, result))
                            return false;
                        if (l == n)
                            return true;
                    } while (comparePrefix(key, pg, l++) >= 0);
                    return false;
                }
            }
            finally
            {
                db.pool.unfix(pg);
            }
            return true;
        }

        internal static int allocate(DatabaseImpl db, int root, ClassDescriptor.FieldType type, OldBtreeKey ins)
        {
            int pageId = db.allocatePage();
            Page pg = db.putPage(pageId);
            setnItems(pg, 1);
            if (type == ClassDescriptor.FieldType.tpString)
            {
                char[] sval = (char[])ins.key.oval;
                int len = sval.Length;
                setSize(pg, len * 2);
                setKeyStrOffs(pg, 0, keySpace - len * 2);
                setKeyStrSize(pg, 0, len);
                setKeyStrOid(pg, 0, ins.oid);
                setKeyStrOid(pg, 1, root);
                setKeyStrChars(pg, keySpace - len * 2, sval);
            }
            else if (type == ClassDescriptor.FieldType.tpArrayOfByte)
            {
                byte[] bval = (byte[])ins.key.oval;
                int len = bval.Length;
                setSize(pg, len);
                setKeyStrOffs(pg, 0, keySpace - len);
                setKeyStrSize(pg, 0, len);
                setKeyStrOid(pg, 0, ins.oid);
                setKeyStrOid(pg, 1, root);
                setKeyBytes(pg, keySpace - len, bval);
            }
            else
            {
                ins.pack(pg, 0);
                setReference(pg, maxItems - 2, root);
            }
            db.pool.unfix(pg);
            return pageId;
        }

        internal static void memcpy(Page dst_pg, int dst_idx, Page src_pg, int src_idx, int len, int itemSize)
        {
            Array.Copy(src_pg.data, firstKeyOffs + src_idx * itemSize, dst_pg.data, firstKeyOffs + dst_idx * itemSize, len * itemSize);
        }

        internal static OldBtreeResult insert(DatabaseImpl db, int pageId, OldBtree tree, OldBtreeKey ins, int height, bool unique, bool overwrite)
        {
            Page pg = db.getPage(pageId);
            OldBtreeResult result;
            int l = 0, n = getnItems(pg), r = n;
            try
            {
                if (tree.FieldType == ClassDescriptor.FieldType.tpString)
                {
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        if (compareStr(ins.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r);
                    if (--height != 0)
                    {
                        result = insert(db, getKeyStrOid(pg, r), tree, ins, height, unique, overwrite);
                        Debug.Assert(result != OldBtreeResult.NotFound);
                        if (result != OldBtreeResult.Overflow)
                        {
                            return result;
                        }
                    }
                    else if (r < n && compareStr(ins.key, pg, r) == 0)
                    {
                        if (overwrite)
                        {
                            db.pool.unfix(pg);
                            pg = null;
                            pg = db.putPage(pageId);
                            setKeyStrOid(pg, r, ins.oid);
                            return OldBtreeResult.Overwrite;
                        }
                        else if (unique)
                        {
                            return OldBtreeResult.Duplicate;
                        }
                    }
                    db.pool.unfix(pg);
                    pg = null;
                    pg = db.putPage(pageId);
                    return insertStrKey(db, pg, r, ins, height);
                }
                else if (tree.FieldType == ClassDescriptor.FieldType.tpArrayOfByte)
                {
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        if (tree.compareByteArrays(ins.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r);
                    if (--height != 0)
                    {
                        result = insert(db, getKeyStrOid(pg, r), tree, ins, height, unique, overwrite);
                        Debug.Assert(result != OldBtreeResult.NotFound);
                        if (result != OldBtreeResult.Overflow)
                        {
                            return result;
                        }
                    }
                    else if (r < n && tree.compareByteArrays(ins.key, pg, r) == 0)
                    {
                        if (overwrite)
                        {
                            db.pool.unfix(pg);
                            pg = null;
                            pg = db.putPage(pageId);
                            setKeyStrOid(pg, r, ins.oid);
                            return OldBtreeResult.Overwrite;
                        }
                        else if (unique)
                        {
                            return OldBtreeResult.Duplicate;
                        }
                    }
                    db.pool.unfix(pg);
                    pg = null;
                    pg = db.putPage(pageId);
                    return insertByteArrayKey(db, pg, r, ins, height);
                }
                else
                {
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        if (compare(ins.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r);
                    /* insert before e[r] */
                    if (--height != 0)
                    {
                        result = insert(db, getReference(pg, maxItems - r - 1), tree, ins, height, unique, overwrite);
                        Debug.Assert(result != OldBtreeResult.NotFound);
                        if (result != OldBtreeResult.Overflow)
                            return result;
                        n += 1;
                    }
                    else if (r < n && compare(ins.key, pg, r) == 0)
                    {
                        if (overwrite)
                        {
                            db.pool.unfix(pg);
                            pg = null;
                            pg = db.putPage(pageId);
                            setReference(pg, maxItems - r - 1, ins.oid);
                            return OldBtreeResult.Overwrite;
                        }
                        else if (unique)
                        {
                            return OldBtreeResult.Duplicate;
                        }
                    }
                    db.pool.unfix(pg);
                    pg = null;
                    pg = db.putPage(pageId);
                    int itemSize = ClassDescriptor.Sizeof[(int)tree.FieldType];
                    int max = keySpace / (4 + itemSize);
                    if (n < max)
                    {
                        memcpy(pg, r + 1, pg, r, n - r, itemSize);
                        memcpy(pg, maxItems - n - 1, pg, maxItems - n, n - r, 4);
                        ins.pack(pg, r);
                        setnItems(pg, getnItems(pg) + 1);
                        return OldBtreeResult.Done;
                    }
                    else
                    {
                        /* page is full then divide page */
                        pageId = db.allocatePage();
                        Page b = db.putPage(pageId);
                        Debug.Assert(n == max);
                        int m = max / 2;
                        if (r < m)
                        {
                            memcpy(b, 0, pg, 0, r, itemSize);
                            memcpy(b, r + 1, pg, r, m - r - 1, itemSize);
                            memcpy(pg, 0, pg, m - 1, max - m + 1, itemSize);
                            memcpy(b, maxItems - r, pg, maxItems - r, r, 4);
                            ins.pack(b, r);
                            memcpy(b, maxItems - m, pg, maxItems - m + 1, m - r - 1, 4);
                            memcpy(pg, maxItems - max + m - 1, pg, maxItems - max, max - m + 1, 4);
                        }
                        else
                        {
                            memcpy(b, 0, pg, 0, m, itemSize);
                            memcpy(pg, 0, pg, m, r - m, itemSize);
                            memcpy(pg, r - m + 1, pg, r, max - r, itemSize);
                            memcpy(b, maxItems - m, pg, maxItems - m, m, 4);
                            memcpy(pg, maxItems - r + m, pg, maxItems - r, r - m, 4);
                            ins.pack(pg, r - m);
                            memcpy(pg, maxItems - max + m - 1, pg, maxItems - max, max - r, 4);
                        }
                        ins.oid = pageId;
                        ins.extract(b, firstKeyOffs + (m - 1) * itemSize, tree.FieldType);
                        if (height == 0)
                        {
                            setnItems(pg, max - m + 1);
                            setnItems(b, m);
                        }
                        else
                        {
                            setnItems(pg, max - m);
                            setnItems(b, m - 1);
                        }
                        db.pool.unfix(b);
                        return OldBtreeResult.Overflow;
                    }
                }
            }
            finally
            {
                if (pg != null)
                    db.pool.unfix(pg);
            }
        }

        internal static OldBtreeResult insertStrKey(DatabaseImpl db, Page pg, int r, OldBtreeKey ins, int height)
        {
            int nItems = getnItems(pg);
            int size = getSize(pg);
            int n = (height != 0) ? nItems + 1 : nItems;
            // insert before e[r]
            char[] sval = (char[])ins.key.oval;
            int len = sval.Length;
            if (size + len * 2 + (n + 1) * strKeySize <= keySpace)
            {
                memcpy(pg, r + 1, pg, r, n - r, strKeySize);
                size += len * 2;
                setKeyStrOffs(pg, r, keySpace - size);
                setKeyStrSize(pg, r, len);
                setKeyStrOid(pg, r, ins.oid);
                setKeyStrChars(pg, keySpace - size, sval);
                nItems += 1;
            }
            else
            {
                // page is full then divide page
                int pageId = db.allocatePage();
                Page b = db.putPage(pageId);
                int moved = 0;
                int inserted = len * 2 + strKeySize;
                int prevDelta = (1 << 31) + 1;

                for (int bn = 0, i = 0; ; bn += 1)
                {
                    int addSize, subSize;
                    int j = nItems - i - 1;
                    int keyLen = getKeyStrSize(pg, i);
                    if (bn == r)
                    {
                        keyLen = len;
                        inserted = 0;
                        addSize = len;
                        if (height == 0)
                        {
                            subSize = 0;
                            j += 1;
                        }
                        else
                        {
                            subSize = getKeyStrSize(pg, i);
                        }
                    }
                    else
                    {
                        addSize = subSize = keyLen;
                        if (height != 0)
                        {
                            if (i + 1 != r)
                            {
                                subSize += getKeyStrSize(pg, i + 1);
                                j -= 1;
                            }
                            else
                            {
                                inserted = 0;
                            }
                        }
                    }
                    int delta = (moved + addSize * 2 + (bn + 1) * strKeySize) - (j * strKeySize + size - subSize * 2 + inserted);
                    if (delta >= -prevDelta)
                    {
                        if (height == 0)
                        {
                            ins.getStr(b, bn - 1);
                        }
                        else
                        {
                            Debug.Assert(moved + (bn + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                            if (bn != r)
                            {
                                ins.getStr(pg, i);
                                setKeyStrOid(b, bn, getKeyStrOid(pg, i));
                                size -= keyLen * 2;
                                i += 1;
                            }
                            else
                            {
                                setKeyStrOid(b, bn, ins.oid);
                            }
                        }
                        nItems = compactifyStrings(pg, i);
                        if (bn < r || (bn == r && height == 0))
                        {
                            memcpy(pg, r - i + 1, pg, r - i, n - r, strKeySize);
                            size += len * 2;
                            nItems += 1;
                            Debug.Assert(size + (n - i + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                            setKeyStrOffs(pg, r - i, keySpace - size);
                            setKeyStrSize(pg, r - i, len);
                            setKeyStrOid(pg, r - i, ins.oid);
                            setKeyStrChars(pg, keySpace - size, sval);
                        }
                        setnItems(b, bn);
                        setSize(b, moved);
                        setSize(pg, size);
                        setnItems(pg, nItems);
                        ins.oid = pageId;
                        db.pool.unfix(b);
                        return OldBtreeResult.Overflow;
                    }
                    moved += keyLen * 2;
                    prevDelta = delta;
                    Debug.Assert(moved + (bn + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                    setKeyStrSize(b, bn, keyLen);
                    setKeyStrOffs(b, bn, keySpace - moved);
                    if (bn == r)
                    {
                        setKeyStrOid(b, bn, ins.oid);
                        setKeyStrChars(b, keySpace - moved, sval);
                    }
                    else
                    {
                        setKeyStrOid(b, bn, getKeyStrOid(pg, i));
                        memcpy(b, keySpace - moved, pg, getKeyStrOffs(pg, i), keyLen * 2, 1);
                        size -= keyLen * 2;
                        i += 1;
                    }
                }
            }
            setnItems(pg, nItems);
            setSize(pg, size);
            return size + strKeySize * (nItems + 1) < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
        }

        internal static OldBtreeResult insertByteArrayKey(DatabaseImpl db, Page pg, int r, OldBtreeKey ins, int height)
        {
            int nItems = getnItems(pg);
            int size = getSize(pg);
            int n = (height != 0) ? nItems + 1 : nItems;
            // insert before e[r]
            byte[] bval = (byte[])ins.key.oval;
            int len = bval.Length;
            if (size + len + (n + 1) * strKeySize <= keySpace)
            {
                memcpy(pg, r + 1, pg, r, n - r, strKeySize);
                size += len;
                setKeyStrOffs(pg, r, keySpace - size);
                setKeyStrSize(pg, r, len);
                setKeyStrOid(pg, r, ins.oid);
                setKeyBytes(pg, keySpace - size, bval);
                nItems += 1;
            }
            else
            {
                // page is full then divide page
                int pageId = db.allocatePage();
                Page b = db.putPage(pageId);
                int moved = 0;
                int inserted = len + strKeySize;
                int prevDelta = (1 << 31) + 1;

                for (int bn = 0, i = 0; ; bn += 1)
                {
                    int addSize, subSize;
                    int j = nItems - i - 1;
                    int keyLen = getKeyStrSize(pg, i);
                    if (bn == r)
                    {
                        keyLen = len;
                        inserted = 0;
                        addSize = len;
                        if (height == 0)
                        {
                            subSize = 0;
                            j += 1;
                        }
                        else
                        {
                            subSize = getKeyStrSize(pg, i);
                        }
                    }
                    else
                    {
                        addSize = subSize = keyLen;
                        if (height != 0)
                        {
                            if (i + 1 != r)
                            {
                                subSize += getKeyStrSize(pg, i + 1);
                                j -= 1;
                            }
                            else
                            {
                                inserted = 0;
                            }
                        }
                    }
                    int delta = (moved + addSize + (bn + 1) * strKeySize) - (j * strKeySize + size - subSize + inserted);
                    if (delta >= -prevDelta)
                    {
                        if (height == 0)
                        {
                            ins.getByteArray(b, bn - 1);
                        }
                        else
                        {
                            Debug.Assert(moved + (bn + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                            if (bn != r)
                            {
                                ins.getByteArray(pg, i);
                                setKeyStrOid(b, bn, getKeyStrOid(pg, i));
                                size -= keyLen;
                                i += 1;
                            }
                            else
                            {
                                setKeyStrOid(b, bn, ins.oid);
                            }
                        }
                        nItems = compactifyByteArrays(pg, i);
                        if (bn < r || (bn == r && height == 0))
                        {
                            memcpy(pg, r - i + 1, pg, r - i, n - r, strKeySize);
                            size += len;
                            nItems += 1;
                            Debug.Assert(size + (n - i + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                            setKeyStrOffs(pg, r - i, keySpace - size);
                            setKeyStrSize(pg, r - i, len);
                            setKeyStrOid(pg, r - i, ins.oid);
                            setKeyBytes(pg, keySpace - size, bval);
                        }
                        setnItems(b, bn);
                        setSize(b, moved);
                        setSize(pg, size);
                        setnItems(pg, nItems);
                        ins.oid = pageId;
                        db.pool.unfix(b);
                        return OldBtreeResult.Overflow;
                    }
                    moved += keyLen;
                    prevDelta = delta;
                    Debug.Assert(moved + (bn + 1) * strKeySize <= keySpace, "String fits in the B-Tree page");
                    setKeyStrSize(b, bn, keyLen);
                    setKeyStrOffs(b, bn, keySpace - moved);
                    if (bn == r)
                    {
                        setKeyStrOid(b, bn, ins.oid);
                        setKeyBytes(b, keySpace - moved, bval);
                    }
                    else
                    {
                        setKeyStrOid(b, bn, getKeyStrOid(pg, i));
                        memcpy(b, keySpace - moved, pg, getKeyStrOffs(pg, i), keyLen, 1);
                        size -= keyLen;
                        i += 1;
                    }
                }
            }
            setnItems(pg, nItems);
            setSize(pg, size);
            return size + strKeySize * (nItems + 1) < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
        }

        internal static int compactifyStrings(Page pg, int m)
        {
            int i, j, offs, len, n = getnItems(pg);
            int[] size = new int[keySpace / 2 + 1];
            int[] index = new int[keySpace / 2 + 1];
            if (m == 0)
                return n;

            int nZeroLengthStrings = 0;
            if (m < 0)
            {
                m = -m;
                for (i = 0; i < n - m; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i) >> 1;
                        size[offs + len] = len;
                        index[offs + len] = i;
                    }
                    else
                    {
                        nZeroLengthStrings += 1;
                    }
                }
                for (; i < n; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i) >> 1;
                        size[offs + len] = len;
                        index[offs + len] = -1;
                    }
                }
            }
            else
            {
                for (i = 0; i < m; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i) >> 1;
                        size[offs + len] = len;
                        index[offs + len] = -1;
                    }
                }
                for (; i < n; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i) >> 1;
                        size[offs + len] = len;
                        index[offs + len] = i - m;
                    }
                    else
                    {
                        nZeroLengthStrings += 1;
                    }
                    setKeyStrOid(pg, i - m, getKeyStrOid(pg, i));
                    setKeyStrSize(pg, i - m, len);
                }
                setKeyStrOid(pg, i - m, getKeyStrOid(pg, i));
            }
            int nItems = n -= m;
            n -= nZeroLengthStrings;
            for (offs = keySpace / 2, i = offs; n != 0; i -= len)
            {
                len = size[i];
                j = index[i];
                if (j >= 0)
                {
                    offs -= len;
                    n -= 1;
                    setKeyStrOffs(pg, j, offs * 2);
                    if (offs != i - len)
                    {
                        memcpy(pg, offs, pg, i - len, len, 2);
                    }
                }
            }
            return nItems;
        }

        internal static int compactifyByteArrays(Page pg, int m)
        {
            int i, j, offs, len, n = getnItems(pg);
            int[] size = new int[keySpace + 1];
            int[] index = new int[keySpace + 1];
            if (m == 0)
                return n;

            int nZeroLengthArrays = 0;
            if (m < 0)
            {
                m = -m;
                for (i = 0; i < n - m; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i);
                        size[offs + len] = len;
                        index[offs + len] = i;
                    }
                    else
                    {
                        nZeroLengthArrays += 1;
                    }
                }
                for (; i < n; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i);
                        size[offs + len] = len;
                        index[offs + len] = -1;
                    }
                }
            }
            else
            {
                for (i = 0; i < m; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i);
                        size[offs + len] = len;
                        index[offs + len] = -1;
                    }
                }
                for (; i < n; i++)
                {
                    len = getKeyStrSize(pg, i);
                    if (len != 0)
                    {
                        offs = getKeyStrOffs(pg, i);
                        size[offs + len] = len;
                        index[offs + len] = i - m;
                    }
                    else
                    {
                        nZeroLengthArrays += 1;
                    }
                    setKeyStrOid(pg, i - m, getKeyStrOid(pg, i));
                    setKeyStrSize(pg, i - m, len);
                }
                setKeyStrOid(pg, i - m, getKeyStrOid(pg, i));
            }
            int nItems = n -= m;
            n -= nZeroLengthArrays;
            for (offs = keySpace, i = offs; n != 0; i -= len)
            {
                len = size[i];
                j = index[i];
                if (j >= 0)
                {
                    offs -= len;
                    n -= 1;
                    setKeyStrOffs(pg, j, offs);
                    if (offs != i - len)
                        memcpy(pg, offs, pg, i - len, len, 1);
                }
            }
            return nItems;
        }

        internal static OldBtreeResult removeStrKey(Page pg, int r)
        {
            int len = getKeyStrSize(pg, r) * 2;
            int offs = getKeyStrOffs(pg, r);
            int size = getSize(pg);
            int nItems = getnItems(pg);
            if ((nItems + 1) * strKeySize >= keySpace)
                memcpy(pg, r, pg, r + 1, nItems - r - 1, strKeySize);
            else
                memcpy(pg, r, pg, r + 1, nItems - r, strKeySize);

            if (len != 0)
            {
                memcpy(pg, keySpace - size + len, pg, keySpace - size, size - keySpace + offs, 1);
                for (int i = nItems; --i >= 0; )
                {
                    if (getKeyStrOffs(pg, i) < offs)
                        setKeyStrOffs(pg, i, getKeyStrOffs(pg, i) + len);
                }
                setSize(pg, size -= len);
            }
            setnItems(pg, nItems - 1);
            return size + strKeySize * nItems < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
        }

        internal static OldBtreeResult removeByteArrayKey(Page pg, int r)
        {
            int len = getKeyStrSize(pg, r);
            int offs = getKeyStrOffs(pg, r);
            int size = getSize(pg);
            int nItems = getnItems(pg);
            if ((nItems + 1) * strKeySize >= keySpace)
                memcpy(pg, r, pg, r + 1, nItems - r - 1, strKeySize);
            else
                memcpy(pg, r, pg, r + 1, nItems - r, strKeySize);

            if (len != 0)
            {
                memcpy(pg, keySpace - size + len, pg, keySpace - size, size - keySpace + offs, 1);
                for (int i = nItems; --i >= 0; )
                {
                    if (getKeyStrOffs(pg, i) < offs)
                        setKeyStrOffs(pg, i, getKeyStrOffs(pg, i) + len);
                }
                setSize(pg, size -= len);
            }
            setnItems(pg, nItems - 1);
            return size + strKeySize * nItems < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
        }

        internal static OldBtreeResult replaceStrKey(DatabaseImpl db, Page pg, int r, OldBtreeKey ins, int height)
        {
            ins.oid = getKeyStrOid(pg, r);
            removeStrKey(pg, r);
            return insertStrKey(db, pg, r, ins, height);
        }

        internal static OldBtreeResult replaceByteArrayKey(DatabaseImpl db, Page pg, int r, OldBtreeKey ins, int height)
        {
            ins.oid = getKeyStrOid(pg, r);
            removeByteArrayKey(pg, r);
            return insertByteArrayKey(db, pg, r, ins, height);
        }

        internal static OldBtreeResult handlePageUnderflow(DatabaseImpl db, Page pg, int r, ClassDescriptor.FieldType type, OldBtreeKey rem, int height)
        {
            int nItems = getnItems(pg);
            if (type == ClassDescriptor.FieldType.tpString)
            {
                Page a = db.putPage(getKeyStrOid(pg, r));
                int an = getnItems(a);
                if (r < nItems)
                {
                    // exists greater page
                    Page b = db.getPage(getKeyStrOid(pg, r + 1));
                    int bn = getnItems(b);
                    int merged_size = (an + bn) * strKeySize + getSize(a) + getSize(b);
                    if (height != 1)
                    {
                        merged_size += getKeyStrSize(pg, r) * 2 + strKeySize * 2;
                    }
                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i, j, k;
                        db.pool.unfix(b);
                        b = db.putPage(getKeyStrOid(pg, r + 1));
                        int size_a = getSize(a);
                        int size_b = getSize(b);
                        int addSize, subSize;
                        if (height != 1)
                        {
                            addSize = getKeyStrSize(pg, r);
                            subSize = getKeyStrSize(b, 0);
                        }
                        else
                        {
                            addSize = subSize = getKeyStrSize(b, 0);
                        }
                        i = 0;
                        int prevDelta = (an * strKeySize + size_a) - (bn * strKeySize + size_b);
                        while (true)
                        {
                            i += 1;
                            int delta = ((an + i) * strKeySize + size_a + addSize * 2) - ((bn - i) * strKeySize + size_b - subSize * 2);
                            if (delta >= 0)
                            {
                                if (delta >= -prevDelta)
                                    i -= 1;
                                break;
                            }
                            size_a += addSize * 2;
                            size_b -= subSize * 2;
                            prevDelta = delta;
                            if (height != 1)
                            {
                                addSize = subSize;
                                subSize = getKeyStrSize(b, i);
                            }
                            else
                            {
                                addSize = subSize = getKeyStrSize(b, i);
                            }
                        }
                        OldBtreeResult result = OldBtreeResult.Done;
                        if (i > 0)
                        {
                            k = i;
                            if (height != 1)
                            {
                                int len = getKeyStrSize(pg, r);
                                setSize(a, getSize(a) + len * 2);
                                setKeyStrOffs(a, an, keySpace - getSize(a));
                                setKeyStrSize(a, an, len);
                                memcpy(a, getKeyStrOffs(a, an), pg, getKeyStrOffs(pg, r), len * 2, 1);
                                k -= 1;
                                an += 1;
                                setKeyStrOid(a, an + k, getKeyStrOid(b, k));
                                setSize(b, getSize(b) - getKeyStrSize(b, k) * 2);
                            }
                            for (j = 0; j < k; j++)
                            {
                                int len = getKeyStrSize(b, j);
                                setSize(a, getSize(a) + len * 2);
                                setSize(b, getSize(b) - len * 2);
                                setKeyStrOffs(a, an, keySpace - getSize(a));
                                setKeyStrSize(a, an, len);
                                setKeyStrOid(a, an, getKeyStrOid(b, j));
                                memcpy(a, getKeyStrOffs(a, an), b, getKeyStrOffs(b, j), len * 2, 1);
                                an += 1;
                            }
                            rem.getStr(b, i - 1);
                            result = replaceStrKey(db, pg, r, rem, height);
                            setnItems(a, an);
                            setnItems(b, compactifyStrings(b, i));
                        }
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return result;
                    }
                    else
                    {
                        // merge page b to a
                        if (height != 1)
                        {
                            int r_len = getKeyStrSize(pg, r);
                            setKeyStrSize(a, an, r_len);
                            setSize(a, getSize(a) + r_len * 2);
                            setKeyStrOffs(a, an, keySpace - getSize(a));
                            memcpy(a, getKeyStrOffs(a, an), pg, getKeyStrOffs(pg, r), r_len * 2, 1);
                            an += 1;
                            setKeyStrOid(a, an + bn, getKeyStrOid(b, bn));
                        }
                        for (int i = 0; i < bn; i++, an++)
                        {
                            setKeyStrSize(a, an, getKeyStrSize(b, i));
                            setKeyStrOffs(a, an, getKeyStrOffs(b, i) - getSize(a));
                            setKeyStrOid(a, an, getKeyStrOid(b, i));
                        }
                        setSize(a, getSize(a) + getSize(b));
                        setnItems(a, an);
                        memcpy(a, keySpace - getSize(a), b, keySpace - getSize(b), getSize(b), 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        db.freePage(getKeyStrOid(pg, r + 1));
                        setKeyStrOid(pg, r + 1, getKeyStrOid(pg, r));
                        return removeStrKey(pg, r);
                    }
                }
                else
                {
                    // page b is before a
                    Page b = db.getPage(getKeyStrOid(pg, r - 1));
                    int bn = getnItems(b);
                    int merged_size = (an + bn) * strKeySize + getSize(a) + getSize(b);
                    if (height != 1)
                        merged_size += getKeyStrSize(pg, r - 1) * 2 + strKeySize * 2;

                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i, j, k, len;
                        db.pool.unfix(b);
                        b = db.putPage(getKeyStrOid(pg, r - 1));
                        int size_a = getSize(a);
                        int size_b = getSize(b);
                        int addSize, subSize;
                        if (height != 1)
                        {
                            addSize = getKeyStrSize(pg, r - 1);
                            subSize = getKeyStrSize(b, bn - 1);
                        }
                        else
                        {
                            addSize = subSize = getKeyStrSize(b, bn - 1);
                        }
                        i = 0;
                        int prevDelta = (an * strKeySize + size_a) - (bn * strKeySize + size_b);
                        while (true)
                        {
                            i += 1;
                            int delta = ((an + i) * strKeySize + size_a + addSize * 2) - ((bn - i) * strKeySize + size_b - subSize * 2);
                            if (delta >= 0)
                            {
                                if (delta >= -prevDelta)
                                    i -= 1;
                                break;
                            }
                            prevDelta = delta;
                            size_a += addSize * 2;
                            size_b -= subSize * 2;
                            if (height != 1)
                            {
                                addSize = subSize;
                                subSize = getKeyStrSize(b, bn - i - 1);
                            }
                            else
                            {
                                addSize = subSize = getKeyStrSize(b, bn - i - 1);
                            }
                        }
                        OldBtreeResult result = OldBtreeResult.Done;
                        if (i > 0)
                        {
                            k = i;
                            Debug.Assert(i < bn);
                            if (height != 1)
                            {
                                setSize(b, getSize(b) - getKeyStrSize(b, bn - k) * 2);
                                memcpy(a, i, a, 0, an + 1, strKeySize);
                                k -= 1;
                                setKeyStrOid(a, k, getKeyStrOid(b, bn));
                                len = getKeyStrSize(pg, r - 1);
                                setKeyStrSize(a, k, len);
                                setSize(a, getSize(a) + len * 2);
                                setKeyStrOffs(a, k, keySpace - getSize(a));
                                memcpy(a, getKeyStrOffs(a, k), pg, getKeyStrOffs(pg, r - 1), len * 2, 1);
                            }
                            else
                            {
                                memcpy(a, i, a, 0, an, strKeySize);
                            }
                            for (j = 0; j < k; j++)
                            {
                                len = getKeyStrSize(b, bn - k + j);
                                setSize(a, getSize(a) + len * 2);
                                setSize(b, getSize(b) - len * 2);
                                setKeyStrOffs(a, j, keySpace - getSize(a));
                                setKeyStrSize(a, j, len);
                                setKeyStrOid(a, j, getKeyStrOid(b, bn - k + j));
                                memcpy(a, getKeyStrOffs(a, j), b, getKeyStrOffs(b, bn - k + j), len * 2, 1);
                            }
                            an += i;
                            setnItems(a, an);
                            rem.getStr(b, bn - k - 1);
                            result = replaceStrKey(db, pg, r - 1, rem, height);
                            setnItems(b, compactifyStrings(b, -i));
                        }
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return result;
                    }
                    else
                    {
                        // merge page b to a
                        if (height != 1)
                        {
                            memcpy(a, bn + 1, a, 0, an + 1, strKeySize);
                            int len = getKeyStrSize(pg, r - 1);
                            setKeyStrSize(a, bn, len);
                            setSize(a, getSize(a) + len * 2);
                            setKeyStrOffs(a, bn, keySpace - getSize(a));
                            setKeyStrOid(a, bn, getKeyStrOid(b, bn));
                            memcpy(a, getKeyStrOffs(a, bn), pg, getKeyStrOffs(pg, r - 1), len * 2, 1);
                            an += 1;
                        }
                        else
                        {
                            memcpy(a, bn, a, 0, an, strKeySize);
                        }
                        for (int i = 0; i < bn; i++)
                        {
                            setKeyStrOid(a, i, getKeyStrOid(b, i));
                            setKeyStrSize(a, i, getKeyStrSize(b, i));
                            setKeyStrOffs(a, i, getKeyStrOffs(b, i) - getSize(a));
                        }
                        an += bn;
                        setnItems(a, an);
                        setSize(a, getSize(a) + getSize(b));
                        memcpy(a, keySpace - getSize(a), b, keySpace - getSize(b), getSize(b), 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        db.freePage(getKeyStrOid(pg, r - 1));
                        return removeStrKey(pg, r - 1);
                    }
                }
            }
            else if (type == ClassDescriptor.FieldType.tpArrayOfByte)
            {
                Page a = db.putPage(getKeyStrOid(pg, r));
                int an = getnItems(a);
                if (r < nItems)
                {
                    // exists greater page
                    Page b = db.getPage(getKeyStrOid(pg, r + 1));
                    int bn = getnItems(b);
                    int merged_size = (an + bn) * strKeySize + getSize(a) + getSize(b);
                    if (height != 1)
                    {
                        merged_size += getKeyStrSize(pg, r) + strKeySize * 2;
                    }
                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i, j, k;
                        db.pool.unfix(b);
                        b = db.putPage(getKeyStrOid(pg, r + 1));
                        int size_a = getSize(a);
                        int size_b = getSize(b);
                        int addSize, subSize;
                        if (height != 1)
                        {
                            addSize = getKeyStrSize(pg, r);
                            subSize = getKeyStrSize(b, 0);
                        }
                        else
                        {
                            addSize = subSize = getKeyStrSize(b, 0);
                        }
                        i = 0;
                        int prevDelta = (an * strKeySize + size_a) - (bn * strKeySize + size_b);
                        while (true)
                        {
                            i += 1;
                            int delta = ((an + i) * strKeySize + size_a + addSize) - ((bn - i) * strKeySize + size_b - subSize);
                            if (delta >= 0)
                            {
                                if (delta >= -prevDelta)
                                    i -= 1;

                                break;
                            }
                            size_a += addSize;
                            size_b -= subSize;
                            prevDelta = delta;
                            if (height != 1)
                            {
                                addSize = subSize;
                                subSize = getKeyStrSize(b, i);
                            }
                            else
                            {
                                addSize = subSize = getKeyStrSize(b, i);
                            }
                        }
                        OldBtreeResult result = OldBtreeResult.Done;
                        if (i > 0)
                        {
                            k = i;
                            if (height != 1)
                            {
                                int len = getKeyStrSize(pg, r);
                                setSize(a, getSize(a) + len);
                                setKeyStrOffs(a, an, keySpace - getSize(a));
                                setKeyStrSize(a, an, len);
                                memcpy(a, getKeyStrOffs(a, an), pg, getKeyStrOffs(pg, r), len, 1);
                                k -= 1;
                                an += 1;
                                setKeyStrOid(a, an + k, getKeyStrOid(b, k));
                                setSize(b, getSize(b) - getKeyStrSize(b, k));
                            }
                            for (j = 0; j < k; j++)
                            {
                                int len = getKeyStrSize(b, j);
                                setSize(a, getSize(a) + len);
                                setSize(b, getSize(b) - len);
                                setKeyStrOffs(a, an, keySpace - getSize(a));
                                setKeyStrSize(a, an, len);
                                setKeyStrOid(a, an, getKeyStrOid(b, j));
                                memcpy(a, getKeyStrOffs(a, an), b, getKeyStrOffs(b, j), len, 1);
                                an += 1;
                            }
                            rem.getByteArray(b, i - 1);
                            result = replaceByteArrayKey(db, pg, r, rem, height);
                            setnItems(a, an);
                            setnItems(b, compactifyByteArrays(b, i));
                        }
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return result;
                    }
                    else
                    {
                        // merge page b to a
                        if (height != 1)
                        {
                            int r_len = getKeyStrSize(pg, r);
                            setKeyStrSize(a, an, r_len);
                            setSize(a, getSize(a) + r_len);
                            setKeyStrOffs(a, an, keySpace - getSize(a));
                            memcpy(a, getKeyStrOffs(a, an), pg, getKeyStrOffs(pg, r), r_len, 1);
                            an += 1;
                            setKeyStrOid(a, an + bn, getKeyStrOid(b, bn));
                        }
                        for (int i = 0; i < bn; i++, an++)
                        {
                            setKeyStrSize(a, an, getKeyStrSize(b, i));
                            setKeyStrOffs(a, an, getKeyStrOffs(b, i) - getSize(a));
                            setKeyStrOid(a, an, getKeyStrOid(b, i));
                        }
                        setSize(a, getSize(a) + getSize(b));
                        setnItems(a, an);
                        memcpy(a, keySpace - getSize(a), b, keySpace - getSize(b), getSize(b), 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        db.freePage(getKeyStrOid(pg, r + 1));
                        setKeyStrOid(pg, r + 1, getKeyStrOid(pg, r));
                        return removeByteArrayKey(pg, r);
                    }
                }
                else
                {
                    // page b is before a
                    Page b = db.getPage(getKeyStrOid(pg, r - 1));
                    int bn = getnItems(b);
                    int merged_size = (an + bn) * strKeySize + getSize(a) + getSize(b);
                    if (height != 1)
                        merged_size += getKeyStrSize(pg, r - 1) + strKeySize * 2;

                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i, j, k, len;
                        db.pool.unfix(b);
                        b = db.putPage(getKeyStrOid(pg, r - 1));
                        int size_a = getSize(a);
                        int size_b = getSize(b);
                        int addSize, subSize;
                        if (height != 1)
                        {
                            addSize = getKeyStrSize(pg, r - 1);
                            subSize = getKeyStrSize(b, bn - 1);
                        }
                        else
                        {
                            addSize = subSize = getKeyStrSize(b, bn - 1);
                        }
                        i = 0;
                        int prevDelta = (an * strKeySize + size_a) - (bn * strKeySize + size_b);
                        while (true)
                        {
                            i += 1;
                            int delta = ((an + i) * strKeySize + size_a + addSize) - ((bn - i) * strKeySize + size_b - subSize);
                            if (delta >= 0)
                            {
                                if (delta >= -prevDelta)
                                    i -= 1;
                                break;
                            }
                            prevDelta = delta;
                            size_a += addSize;
                            size_b -= subSize;
                            if (height != 1)
                            {
                                addSize = subSize;
                                subSize = getKeyStrSize(b, bn - i - 1);
                            }
                            else
                            {
                                addSize = subSize = getKeyStrSize(b, bn - i - 1);
                            }
                        }
                        OldBtreeResult result = OldBtreeResult.Done;
                        if (i > 0)
                        {
                            k = i;
                            Debug.Assert(i < bn);
                            if (height != 1)
                            {
                                setSize(b, getSize(b) - getKeyStrSize(b, bn - k));
                                memcpy(a, i, a, 0, an + 1, strKeySize);
                                k -= 1;
                                setKeyStrOid(a, k, getKeyStrOid(b, bn));
                                len = getKeyStrSize(pg, r - 1);
                                setKeyStrSize(a, k, len);
                                setSize(a, getSize(a) + len);
                                setKeyStrOffs(a, k, keySpace - getSize(a));
                                memcpy(a, getKeyStrOffs(a, k), pg, getKeyStrOffs(pg, r - 1), len, 1);
                            }
                            else
                            {
                                memcpy(a, i, a, 0, an, strKeySize);
                            }
                            for (j = 0; j < k; j++)
                            {
                                len = getKeyStrSize(b, bn - k + j);
                                setSize(a, getSize(a) + len);
                                setSize(b, getSize(b) - len);
                                setKeyStrOffs(a, j, keySpace - getSize(a));
                                setKeyStrSize(a, j, len);
                                setKeyStrOid(a, j, getKeyStrOid(b, bn - k + j));
                                memcpy(a, getKeyStrOffs(a, j), b, getKeyStrOffs(b, bn - k + j), len, 1);
                            }
                            an += i;
                            setnItems(a, an);
                            rem.getByteArray(b, bn - k - 1);
                            result = replaceByteArrayKey(db, pg, r - 1, rem, height);
                            setnItems(b, compactifyByteArrays(b, -i));
                        }
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return result;
                    }
                    else
                    {
                        // merge page b to a
                        if (height != 1)
                        {
                            memcpy(a, bn + 1, a, 0, an + 1, strKeySize);
                            int len = getKeyStrSize(pg, r - 1);
                            setKeyStrSize(a, bn, len);
                            setSize(a, getSize(a) + len);
                            setKeyStrOffs(a, bn, keySpace - getSize(a));
                            setKeyStrOid(a, bn, getKeyStrOid(b, bn));
                            memcpy(a, getKeyStrOffs(a, bn), pg, getKeyStrOffs(pg, r - 1), len, 1);
                            an += 1;
                        }
                        else
                        {
                            memcpy(a, bn, a, 0, an, strKeySize);
                        }
                        for (int i = 0; i < bn; i++)
                        {
                            setKeyStrOid(a, i, getKeyStrOid(b, i));
                            setKeyStrSize(a, i, getKeyStrSize(b, i));
                            setKeyStrOffs(a, i, getKeyStrOffs(b, i) - getSize(a));
                        }
                        an += bn;
                        setnItems(a, an);
                        setSize(a, getSize(a) + getSize(b));
                        memcpy(a, keySpace - getSize(a), b, keySpace - getSize(b), getSize(b), 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        db.freePage(getKeyStrOid(pg, r - 1));
                        return removeByteArrayKey(pg, r - 1);
                    }
                }
            }
            else
            {
                Page a = db.putPage(getReference(pg, maxItems - r - 1));
                int an = getnItems(a);
                int itemSize = ClassDescriptor.Sizeof[(int)type];
                if (r < nItems)
                {
                    // exists greater page
                    Page b = db.getPage(getReference(pg, maxItems - r - 2));
                    int bn = getnItems(b);
                    Debug.Assert(bn >= an);
                    if (height != 1)
                    {
                        memcpy(a, an, pg, r, 1, itemSize);
                        an += 1;
                        bn += 1;
                    }
                    int merged_size = (an + bn) * (4 + itemSize);
                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i = bn - ((an + bn) >> 1);
                        db.pool.unfix(b);
                        b = db.putPage(getReference(pg, maxItems - r - 2));
                        memcpy(a, an, b, 0, i, itemSize);
                        memcpy(b, 0, b, i, bn - i, itemSize);
                        memcpy(a, maxItems - an - i, b, maxItems - i, i, 4);
                        memcpy(b, maxItems - bn + i, b, maxItems - bn, bn - i, 4);
                        memcpy(pg, r, a, an + i - 1, 1, itemSize);
                        setnItems(b, getnItems(b) - i);
                        setnItems(a, getnItems(a) + i);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return OldBtreeResult.Done;
                    }
                    else
                    {
                        // merge page b to a  
                        memcpy(a, an, b, 0, bn, itemSize);
                        memcpy(a, maxItems - an - bn, b, maxItems - bn, bn, 4);
                        db.freePage(getReference(pg, maxItems - r - 2));
                        memcpy(pg, maxItems - nItems, pg, maxItems - nItems - 1, nItems - r - 1, 4);
                        memcpy(pg, r, pg, r + 1, nItems - r - 1, itemSize);
                        setnItems(a, getnItems(a) + bn);
                        setnItems(pg, nItems - 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return nItems * (itemSize + 4) < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
                    }
                }
                else
                {
                    // page b is before a
                    Page b = db.getPage(getReference(pg, maxItems - r));
                    int bn = getnItems(b);
                    Debug.Assert(bn >= an);
                    if (height != 1)
                    {
                        an += 1;
                        bn += 1;
                    }
                    int merged_size = (an + bn) * (4 + itemSize);
                    if (merged_size > keySpace)
                    {
                        // reallocation of nodes between pages a and b
                        int i = bn - ((an + bn) >> 1);
                        db.pool.unfix(b);
                        b = db.putPage(getReference(pg, maxItems - r));
                        memcpy(a, i, a, 0, an, itemSize);
                        memcpy(a, 0, b, bn - i, i, itemSize);
                        memcpy(a, maxItems - an - i, a, maxItems - an, an, 4);
                        memcpy(a, maxItems - i, b, maxItems - bn, i, 4);
                        if (height != 1)
                        {
                            memcpy(a, i - 1, pg, r - 1, 1, itemSize);
                        }
                        memcpy(pg, r - 1, b, bn - i - 1, 1, itemSize);
                        setnItems(b, getnItems(b) - i);
                        setnItems(a, getnItems(a) + i);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return OldBtreeResult.Done;
                    }
                    else
                    {
                        // merge page b to a
                        memcpy(a, bn, a, 0, an, itemSize);
                        memcpy(a, 0, b, 0, bn, itemSize);
                        memcpy(a, maxItems - an - bn, a, maxItems - an, an, 4);
                        memcpy(a, maxItems - bn, b, maxItems - bn, bn, 4);
                        if (height != 1)
                            memcpy(a, bn - 1, pg, r - 1, 1, itemSize);

                        db.freePage(getReference(pg, maxItems - r));
                        setReference(pg, maxItems - r, getReference(pg, maxItems - r - 1));
                        setnItems(a, getnItems(a) + bn);
                        setnItems(pg, nItems - 1);
                        db.pool.unfix(a);
                        db.pool.unfix(b);
                        return nItems * (itemSize + 4) < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
                    }
                }
            }
        }

        internal static OldBtreeResult remove(DatabaseImpl db, int pageId, OldBtree tree, OldBtreeKey rem, int height)
        {
            Page pg = db.getPage(pageId);
            try
            {
                int i, n = getnItems(pg), l = 0, r = n;

                if (tree.FieldType == ClassDescriptor.FieldType.tpString)
                {
                    while (l < r)
                    {
                        i = (l + r) >> 1;
                        if (compareStr(rem.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;
                    }
                    if (--height != 0)
                    {
                        do
                        {
                            switch (remove(db, getKeyStrOid(pg, r), tree, rem, height))
                            {
                                case OldBtreeResult.Underflow:
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return handlePageUnderflow(db, pg, r, tree.FieldType, rem, height);

                                case OldBtreeResult.Done:
                                    return OldBtreeResult.Done;

                                case OldBtreeResult.Overflow:
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return insertStrKey(db, pg, r, rem, height);

                            }
                        }
                        while (++r <= n);
                    }
                    else
                    {
                        while (r < n)
                        {
                            if (compareStr(rem.key, pg, r) == 0)
                            {
                                int oid = getKeyStrOid(pg, r);
                                if (oid == rem.oid || rem.oid == 0)
                                {
                                    rem.oldOid = oid;
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return removeStrKey(pg, r);
                                }
                            }
                            else
                            {
                                break;
                            }
                            r += 1;
                        }
                    }
                }
                else if (tree.FieldType == ClassDescriptor.FieldType.tpArrayOfByte)
                {
                    while (l < r)
                    {
                        i = (l + r) >> 1;
                        if (tree.compareByteArrays(rem.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;
                    }
                    if (--height != 0)
                    {
                        do
                        {
                            switch (remove(db, getKeyStrOid(pg, r), tree, rem, height))
                            {
                                case OldBtreeResult.Underflow:
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return handlePageUnderflow(db, pg, r, tree.FieldType, rem, height);

                                case OldBtreeResult.Done:
                                    return OldBtreeResult.Done;

                                case OldBtreeResult.Overflow:
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return insertByteArrayKey(db, pg, r, rem, height);

                            }
                        }
                        while (++r <= n);
                    }
                    else
                    {
                        while (r < n)
                        {
                            if (tree.compareByteArrays(rem.key, pg, r) == 0)
                            {
                                int oid = getKeyStrOid(pg, r);
                                if (oid == rem.oid || rem.oid == 0)
                                {
                                    rem.oldOid = oid;
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    return removeByteArrayKey(pg, r);
                                }
                            }
                            else
                            {
                                break;
                            }
                            r += 1;
                        }
                    }
                }
                else // scalars
                {
                    int itemSize = ClassDescriptor.Sizeof[(int)tree.FieldType];
                    while (l < r)
                    {
                        i = (l + r) >> 1;
                        if (compare(rem.key, pg, i) > 0)
                            l = i + 1;
                        else
                            r = i;

                    }
                    if (--height == 0)
                    {
                        int oid = rem.oid;
                        while (r < n)
                        {
                            if (compare(rem.key, pg, r) == 0)
                            {
                                if (getReference(pg, maxItems - r - 1) == oid || oid == 0)
                                {
                                    rem.oldOid = getReference(pg, maxItems - r - 1);
                                    db.pool.unfix(pg);
                                    pg = null;
                                    pg = db.putPage(pageId);
                                    memcpy(pg, r, pg, r + 1, n - r - 1, itemSize);
                                    memcpy(pg, maxItems - n + 1, pg, maxItems - n, n - r - 1, 4);
                                    setnItems(pg, --n);
                                    return n * (itemSize + 4) < keySpace / 2 ? OldBtreeResult.Underflow : OldBtreeResult.Done;
                                }
                            }
                            else
                            {
                                break;
                            }
                            r += 1;
                        }
                        return OldBtreeResult.NotFound;
                    }
                    do
                    {
                        switch (remove(db, getReference(pg, maxItems - r - 1), tree, rem, height))
                        {
                            case OldBtreeResult.Underflow:
                                db.pool.unfix(pg);
                                pg = db.putPage(pageId);
                                return handlePageUnderflow(db, pg, r, tree.FieldType, rem, height);

                            case OldBtreeResult.Done:
                                return OldBtreeResult.Done;

                        }
                    }
                    while (++r <= n);
                }
                return OldBtreeResult.NotFound;
            }
            finally
            {
                if (pg != null)
                {
                    db.pool.unfix(pg);

                }
            }
        }

        internal static void purge(DatabaseImpl db, int pageId, ClassDescriptor.FieldType type, int height)
        {
            if (--height != 0)
            {
                Page pg = db.getPage(pageId);
                int n = getnItems(pg) + 1;
                if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                {
                    // page of strings
                    while (--n >= 0)
                    {
                        purge(db, getKeyStrOid(pg, n), type, height);
                    }
                }
                else
                {
                    while (--n >= 0)
                    {
                        purge(db, getReference(pg, maxItems - n - 1), type, height);
                    }
                }
                db.pool.unfix(pg);
            }
            db.freePage(pageId);
        }

        internal static int traverseForward(DatabaseImpl db, int pageId, ClassDescriptor.FieldType type, int height, IPersistent[] result, int pos)
        {
            Page pg = db.getPage(pageId);
            int oid;
            try
            {
                int i, n = getnItems(pg);
                if (--height != 0)
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    {
                        // page of strings
                        for (i = 0; i <= n; i++)
                        {
                            pos = traverseForward(db, getKeyStrOid(pg, i), type, height, result, pos);
                        }
                    }
                    else
                    {
                        for (i = 0; i <= n; i++)
                        {
                            pos = traverseForward(db, getReference(pg, maxItems - i - 1), type, height, result, pos);
                        }
                    }
                }
                else
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    {
                        // page of strings
                        for (i = 0; i < n; i++)
                        {
                            oid = getKeyStrOid(pg, i);
                            result[pos++] = db.lookupObject(oid, null);
                        }
                    }
                    else
                    {
                        // page of scalars
                        for (i = 0; i < n; i++)
                        {
                            oid = getReference(pg, maxItems - 1 - i);
                            result[pos++] = db.lookupObject(oid, null);
                        }
                    }
                }
                return pos;
            }
            finally
            {
                db.pool.unfix(pg);
            }
        }

        internal static int markPage(DatabaseImpl db, int pageId, ClassDescriptor.FieldType type, int height)
        {
            Page pg = db.getGCPage(pageId);
            int nPages = 1;
            try
            {
                int i, n = getnItems(pg);
                if (--height != 0)
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    { // page of strings
                        for (i = 0; i <= n; i++)
                        {
                            nPages += markPage(db, getKeyStrOid(pg, i), type, height);
                        }
                    }
                    else
                    {
                        for (i = 0; i <= n; i++)
                        {
                            nPages += markPage(db, getReference(pg, maxItems - i - 1), type, height);
                        }
                    }
                }
                else
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    { // page of strings
                        for (i = 0; i < n; i++)
                        {
                            db.markOid(getKeyStrOid(pg, i));
                        }
                    }
                    else
                    { // page of scalars
                        for (i = 0; i < n; i++)
                        {
                            db.markOid(getReference(pg, maxItems - 1 - i));
                        }
                    }
                }
            }
            finally
            {
                db.pool.unfix(pg);
            }
            return nPages;
        }

#if WITH_XML
        internal static void exportPage(DatabaseImpl db, XmlExporter exporter, int pageId, ClassDescriptor.FieldType type, int height)
        {
            Page pg = db.getPage(pageId);
            try
            {
                int i, n = getnItems(pg);
                if (--height != 0)
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    {
                        // page of strings
                        for (i = 0; i <= n; i++)
                        {
                            exportPage(db, exporter, getKeyStrOid(pg, i), type, height);
                        }
                    }
                    else
                    {
                        for (i = 0; i <= n; i++)
                        {
                            exportPage(db, exporter, getReference(pg, maxItems - i - 1), type, height);
                        }
                    }
                }
                else
                {
                    if (type == ClassDescriptor.FieldType.tpString || type == ClassDescriptor.FieldType.tpArrayOfByte)
                    {
                        // page of strings
                        for (i = 0; i < n; i++)
                        {
                            exporter.exportAssoc(getKeyStrOid(pg, i), pg.data, OldBtreePage.firstKeyOffs + OldBtreePage.getKeyStrOffs(pg, i), OldBtreePage.getKeyStrSize(pg, i), type);
                        }
                    }
                    else
                    {
                        // page of scalars
                        for (i = 0; i < n; i++)
                        {
                            exporter.exportAssoc(getReference(pg, maxItems - 1 - i), pg.data, OldBtreePage.firstKeyOffs + i * ClassDescriptor.Sizeof[(int)type], ClassDescriptor.Sizeof[(int)type], type);
                        }
                    }
                }
            }
            finally
            {
                db.pool.unfix(pg);
            }
        }
#endif
    }
}
#endif
