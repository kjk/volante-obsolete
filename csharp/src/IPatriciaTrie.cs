#if WITH_PATRICIA
using System;
using System.Collections;
using System.Collections.Generic;

namespace Volante
{
    /// <summary> 
    /// Trie is a kind of tree where each node holds a common part of one or more keys. 
    /// Patricia trie is one of the many existing variants of the trie, which adds path
    /// compression by grouping common sequences of nodes together.
    /// This structure provides a very efficient way of storing values while maintaining
    /// the lookup time for a key in O(N) in the worst case, where N is the length of
    /// the longest key. This structure has it's main use in IP routing software, but
    /// can provide an interesting alternative to other structures such as hashtables
    /// when memory space is of concern.</summary>
    public interface IPatriciaTrie<T> : IPersistent, IResource, ICollection<T> where T : class,IPersistent
    {
        /// <summary> 
        /// Add new key to the trie
        /// </summary>
        /// <param name="key">bit vector</param>
        /// <param name="obj">persistent object associated with this key</param>
        /// <returns>previous object associtated with this key or <code>null</code> if there
        /// was no such object</returns>
        T Add(PatriciaTrieKey key, T obj);

        /// <summary>
        /// Find best match with specified key
        /// </summary>
        /// <param name="key">bit vector</param>
        /// <returns>object associated with this deepest possible match with specified key</returns>
        T FindBestMatch(PatriciaTrieKey key);

        /// <summary>
        /// Find exact match with specified key
        /// </summary>
        /// <param name="key">bit vector</param>
        /// <returns>object associated with this key or NULL if match is not found</returns>
        T FindExactMatch(PatriciaTrieKey key);

        /// <summary>
        /// Removes key from the triesKFind exact match with specified key
        /// </summary>
        /// <param name="key">bit vector</param>
        /// <returns>object associated with removed key or <code>null</code> if such key is not found</returns>
        T Remove(PatriciaTrieKey key);
    }
}
#endif
