using System;
using Perst;

namespace Perst.Impl
{
    class PTrie : PersistentResource, PatriciaTrie 
    { 
        private PTrieNode rootZero;
        private PTrieNode rootOne;
        private int       count;

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

        public IPersistent Add(PatriciaTrieKey key, IPersistent obj) 
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
    
        public IPersistent FindBestMatch(PatriciaTrieKey key) 
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
    

        public IPersistent FindExactMatch(PatriciaTrieKey key) 
        {
            if (firstDigit(key.mask, key.length) == 1) 
            {
                if (rootOne != null) 
                { 
                    return rootOne.findExactMatch(key.mask, key.length);
                } 
            } 
            else 
            { 
                if (rootZero != null) 
                { 
                    return rootZero.findExactMatch(key.mask, key.length);
                } 
            }
            return null;
        }
    
        public IPersistent Remove(PatriciaTrieKey key) 
        { 
            if (firstDigit(key.mask, key.length) == 1) 
            {
                if (rootOne != null) 
                { 
                    IPersistent obj = rootOne.remove(key.mask, key.length);
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
                    IPersistent obj = rootZero.remove(key.mask, key.length);
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

        public void Clear() 
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
            internal ulong       key;
            internal int         keyLength;
            internal IPersistent obj;
            internal PTrieNode   childZero;
            internal PTrieNode   childOne;

            internal PTrieNode(ulong key, int keyLength, IPersistent obj)
            {
                this.obj = obj;
                this.key = key;
                this.keyLength = keyLength; 
            }

            PTrieNode() {}

            internal IPersistent add(ulong key, int keyLength, IPersistent obj) 
            {
                if (key == this.key && keyLength == this.keyLength) 
                {
                    Modify();
                    // the new is matched exactly by this node's key, so just replace the node object
                    IPersistent prevObj = this.obj;
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
                    IPersistent prevObj = this.obj;
                    this.obj = obj;
                    return prevObj;
                }            
            }
    
        
            internal IPersistent findBestMatch(ulong key, int keyLength) 
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
                        { 
                            return childOne.findBestMatch(keyDiff, keyLengthDiff);
                        }
                    } 
                    else 
                    {
                        if (childZero != null) 
                        { 
                            return childZero.findBestMatch(keyDiff, keyLengthDiff);
                        }
                    }
                }
                return obj;
            }
				
            internal IPersistent findExactMatch(ulong key, int keyLength) 
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
                            { 
                                return childOne.findBestMatch(keyDiff, keyLengthDiff);
                            }
                        } 
                        else 
                        {
                            if (childZero != null) 
                            { 
                                return childZero.findBestMatch(keyDiff, keyLengthDiff);
                            } 
                        }
                    }
                }
                return null;
            }		

            internal bool isNotUsed() 
            { 
                return obj == null && childOne == null && childZero == null;
            }

            internal IPersistent remove(ulong key, int keyLength) 
            {             
                if (keyLength >= this.keyLength) 
                { 
                    if (key == this.key && keyLength == this.keyLength) 
                    { 
                        IPersistent obj = this.obj;
                        this.obj = null;
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
                            { 
                                IPersistent obj = childOne.findBestMatch(keyDiff, keyLengthDiff);
                                if (obj != null) 
                                { 
                                    if (childOne.isNotUsed()) 
                                    {
                                        Modify();
                                        childOne.Deallocate();
                                        childOne = null;
                                    }
                                    return obj;                                    
                                }
                            }
                        } 
                        else 
                        {
                            if (childZero != null) 
                            { 
                                IPersistent obj = childZero.findBestMatch(keyDiff, keyLengthDiff);
                                if (obj != null) 
                                { 
                                    if (childZero.isNotUsed()) 
                                    { 
                                        Modify();
                                        childZero.Deallocate();
                                        childZero = null;
                                    }
                                    return obj;                                    
                                }
                            } 
                        }
                    }
                }
                return null;
            }		

            public override void Deallocate() 
            {
                if (childOne != null) 
                { 
                    childOne.Deallocate();
                }
                if (childZero != null) 
                { 
                    childZero.Deallocate();
                }
                base.Deallocate();
            }
        }
    }
}
