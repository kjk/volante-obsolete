#if WITH_PATRICIA
using System;
using System.Collections;
using System.Collections.Generic;
using Volante;

namespace Volante.Impl
{
    class PTrie<T> : PersistentCollection<T>, IPatriciaTrie<T> where T : class, IPersistent
    {
        private PTrieNode rootZero;
        private PTrieNode rootOne;
        private int count;

        public override IEnumerator<T> GetEnumerator()
        {
            List<T> list = new List<T>();
            fill(list, rootZero);
            fill(list, rootOne);
            return list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private static void fill(List<T> list, PTrieNode node)
        {
            if (null == node)
                return;

            list.Add(node.obj);
            fill(list, node.childZero);
            fill(list, node.childOne);
        }

        public override int Count
        {
            get
            {
                return count;
            }
        }

        private static int firstDigit(ulong key, int keyLength)
        {
            return (int)(key >> (keyLength - 1)) & 1;
        }

        private static int getCommonPart(ulong keyA, int keyLengthA, ulong keyB, int keyLengthB)
        {
            // truncate the keys so they are the same size (discard low bits)
            if (keyLengthA > keyLengthB)
            {
                keyA >>= keyLengthA - keyLengthB;
                keyLengthA = keyLengthB;
            }
            else
            {
                keyB >>= keyLengthB - keyLengthA;
                keyLengthB = keyLengthA;
            }
            // now get common part
            ulong diff = keyA ^ keyB;

            // finally produce common key part
            int count = 0;
            while (diff != 0)
            {
                diff >>= 1;
                count += 1;
            }
            return keyLengthA - count;
        }

        public T Add(PatriciaTrieKey key, T obj)
        {
            Modify();
            count += 1;

            if (firstDigit(key.mask, key.length) == 1)
            {
                if (rootOne != null)
                {
                    return rootOne.add(key.mask, key.length, obj);
                }
                else
                {
                    rootOne = new PTrieNode(key.mask, key.length, obj);
                    return null;
                }
            }
            else
            {
                if (rootZero != null)
                {
                    return rootZero.add(key.mask, key.length, obj);
                }
                else
                {
                    rootZero = new PTrieNode(key.mask, key.length, obj);
                    return null;
                }
            }
        }

        public T FindBestMatch(PatriciaTrieKey key)
        {
            if (firstDigit(key.mask, key.length) == 1)
            {
                if (rootOne != null)
                {
                    return rootOne.findBestMatch(key.mask, key.length);
                }
            }
            else
            {
                if (rootZero != null)
                {
                    return rootZero.findBestMatch(key.mask, key.length);
                }
            }
            return null;
        }

        public T FindExactMatch(PatriciaTrieKey key)
        {
            if (firstDigit(key.mask, key.length) == 1)
            {
                if (rootOne != null)
                    return rootOne.findExactMatch(key.mask, key.length);
            }
            else
            {
                if (rootZero != null)
                    return rootZero.findExactMatch(key.mask, key.length);
            }
            return null;
        }

        public T Remove(PatriciaTrieKey key)
        {
            T obj;
            if (firstDigit(key.mask, key.length) == 1)
            {
                if (rootOne != null)
                {
                    obj = rootOne.remove(key.mask, key.length);
                    if (obj != null)
                    {
                        Modify();
                        count -= 1;
                        if (rootOne.isNotUsed())
                        {
                            rootOne.Deallocate();
                            rootOne = null;
                        }
                        return obj;
                    }
                }
            }
            else
            {
                if (rootZero != null)
                {
                    obj = rootZero.remove(key.mask, key.length);
                    if (obj != null)
                    {
                        Modify();
                        count -= 1;
                        if (rootZero.isNotUsed())
                        {
                            rootZero.Deallocate();
                            rootZero = null;
                        }
                        return obj;
                    }
                }
            }
            return null;
        }

        public override void Clear()
        {
            if (rootOne != null)
            {
                rootOne.Deallocate();
                rootOne = null;
            }
            if (rootZero != null)
            {
                rootZero.Deallocate();
                rootZero = null;
            }
            count = 0;
        }

        class PTrieNode : Persistent
        {
            internal ulong key;
            internal int keyLength;
            internal T obj;
            internal PTrieNode childZero;
            internal PTrieNode childOne;

            internal PTrieNode(ulong key, int keyLength, T obj)
            {
                this.obj = obj;
                this.key = key;
                this.keyLength = keyLength;
            }

            PTrieNode() { }

            internal T add(ulong key, int keyLength, T obj)
            {
                T prevObj;
                if (key == this.key && keyLength == this.keyLength)
                {
                    Modify();
                    // the new is matched exactly by this node's key, so just replace the node object
                    prevObj = this.obj;
                    this.obj = obj;
                    return prevObj;
                }
                int keyLengthCommon = getCommonPart(key, keyLength, this.key, this.keyLength);
                int keyLengthDiff = this.keyLength - keyLengthCommon;
                ulong keyCommon = key >> (keyLength - keyLengthCommon);
                ulong keyDiff = this.key - (keyCommon << keyLengthDiff);
                // process diff with this node's key, if any
                if (keyLengthDiff > 0)
                {
                    Modify();
                    // create a new node with the diff
                    PTrieNode newNode = new PTrieNode(keyDiff, keyLengthDiff, this.obj);
                    // transfer infos of this node to the new node
                    newNode.childZero = childZero;
                    newNode.childOne = childOne;

                    // update this node to hold common part
                    this.key = keyCommon;
                    this.keyLength = keyLengthCommon;
                    this.obj = null;

                    // and set the new node as child of this node
                    if (firstDigit(keyDiff, keyLengthDiff) == 1)
                    {
                        childZero = null;
                        childOne = newNode;
                    }
                    else
                    {
                        childZero = newNode;
                        childOne = null;
                    }
                }

                // process diff with the new key, if any
                if (keyLength > keyLengthCommon)
                {
                    // get diff with the new key
                    keyLengthDiff = keyLength - keyLengthCommon;
                    keyDiff = key - (keyCommon << keyLengthDiff);

                    // get which child we use as insertion point and do insertion (recursive)
                    if (firstDigit(keyDiff, keyLengthDiff) == 1)
                    {
                        if (childOne != null)
                        {
                            return childOne.add(keyDiff, keyLengthDiff, obj);
                        }
                        else
                        {
                            Modify();
                            childOne = new PTrieNode(keyDiff, keyLengthDiff, obj);
                            return null;
                        }
                    }
                    else
                    {
                        if (childZero != null)
                        {
                            return childZero.add(keyDiff, keyLengthDiff, obj);
                        }
                        else
                        {
                            Modify();
                            childZero = new PTrieNode(keyDiff, keyLengthDiff, obj);
                            return null;
                        }
                    }
                }
                else
                { // the new key was containing within this node's original key, so just set this node as terminator
                    prevObj = this.obj;
                    this.obj = obj;
                    return prevObj;
                }
            }

            internal T findBestMatch(ulong key, int keyLength)
            {
                if (keyLength > this.keyLength)
                {
                    int keyLengthCommon = getCommonPart(key, keyLength, this.key, this.keyLength);
                    int keyLengthDiff = keyLength - keyLengthCommon;
                    ulong keyCommon = key >> keyLengthDiff;
                    ulong keyDiff = key - (keyCommon << keyLengthDiff);

                    if (firstDigit(keyDiff, keyLengthDiff) == 1)
                    {
                        if (childOne != null)
                            return childOne.findBestMatch(keyDiff, keyLengthDiff);
                    }
                    else
                    {
                        if (childZero != null)
                            return childZero.findBestMatch(keyDiff, keyLengthDiff);
                    }
                }
                return obj;
            }

            internal T findExactMatch(ulong key, int keyLength)
            {
                if (keyLength >= this.keyLength)
                {
                    if (key == this.key && keyLength == this.keyLength)
                    {
                        return obj;
                    }
                    else
                    {
                        int keyLengthCommon = getCommonPart(key, keyLength, this.key, this.keyLength);
                        int keyLengthDiff = keyLength - keyLengthCommon;
                        ulong keyCommon = key >> keyLengthDiff;
                        ulong keyDiff = key - (keyCommon << keyLengthDiff);

                        if (firstDigit(keyDiff, keyLengthDiff) == 1)
                        {
                            if (childOne != null)
                                return childOne.findBestMatch(keyDiff, keyLengthDiff);
                        }
                        else
                        {
                            if (childZero != null)
                                return childZero.findBestMatch(keyDiff, keyLengthDiff);
                        }
                    }
                }
                return null;
            }

            internal bool isNotUsed()
            {
                return obj == null && childOne == null && childZero == null;
            }

            internal T remove(ulong key, int keyLength)
            {
                T obj;
                if (keyLength < this.keyLength)
                    return null;

                if (key == this.key && keyLength == this.keyLength)
                {
                    obj = this.obj;
                    this.obj = null;
                    return obj;
                }

                int keyLengthCommon = getCommonPart(key, keyLength, this.key, this.keyLength);
                int keyLengthDiff = keyLength - keyLengthCommon;
                ulong keyCommon = key >> keyLengthDiff;
                ulong keyDiff = key - (keyCommon << keyLengthDiff);

                if (firstDigit(keyDiff, keyLengthDiff) == 1)
                {
                    if (childOne == null)
                        return null;

                    obj = childOne.findBestMatch(keyDiff, keyLengthDiff);
                    if (obj == null)
                        return null;
  
                    if (childOne.isNotUsed())
                    {
                        Modify();
                        childOne.Deallocate();
                        childOne = null;
                    }
                    return obj;
                }

                if (childZero == null)
                    return null;

                obj = childZero.findBestMatch(keyDiff, keyLengthDiff);
                if (obj == null)
                    return null;

                if (childZero.isNotUsed())
                {
                    Modify();
                    childZero.Deallocate();
                    childZero = null;
                }
                return obj;
            }

            public override void Deallocate()
            {
                if (childOne != null)
                    childOne.Deallocate();

                if (childZero != null)
                    childZero.Deallocate();

                base.Deallocate();
            }
        }
    }
}
#endif
