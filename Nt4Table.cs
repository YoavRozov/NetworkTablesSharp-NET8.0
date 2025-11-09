using System;
using System.Collections.Generic;

namespace NetworkTablesSharp
{
    /// <summary>
    /// Represents a logical "table" (path prefix) in the NT4 topic space.
    /// </summary>
    public sealed class Nt4Table
    {
        private readonly Nt4Source _source;
        private readonly string _tablePath; // normalized: starts with '/', no trailing '/'

        internal Nt4Table(Nt4Source source, string tablePath)
        {
            _source = source;
            _tablePath = NormalizeTablePath(tablePath);
        }

        private static string NormalizeTablePath(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return string.Empty;
            var p = path.Replace('\\', '/').Trim();
            if (!p.StartsWith("/")) p = "/" + p;
            // remove trailing slash (except if root "/")
            if (p.Length > 1 && p.EndsWith("/")) p = p.TrimEnd('/');
            return p;
        }

        private string FullKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("Key cannot be empty.", nameof(key));
            var k = key.Replace('\\', '/');
            if (k.StartsWith("/")) k = k.TrimStart('/');
            // root table "" behaves like global
            return string.IsNullOrEmpty(_tablePath) ? "/" + k : _tablePath + "/" + k;
        }

        /// <summary>
        /// Publish a topic (create it with type and optional properties) within this table.
        /// </summary>
        public void PublishTopic(string key, string type, Dictionary<string, object>? properties = null)
        {
            _source.Client.PublishTopic(FullKey(key), type, properties ?? new Dictionary<string, object>());
        }

        /// <summary>
        /// Publish a value to a topic in this table.
        /// </summary>
        public void PublishValue(string key, object value)
        {
            _source.Client.PublishValue(FullKey(key), value);
        }

        /// <summary>
        /// Subscribe to one key in this table.
        /// </summary>
        public int Subscribe(string key, double period = 0.1, bool all = false, bool topicsOnly = false, bool prefix = false)
        {
            return _source.Client.Subscribe(FullKey(key), period, all, topicsOnly, prefix);
        }

        /// <summary>
        /// Subscribe to many keys in this table (array of keys relative to the table).
        /// </summary>
        public int Subscribe(string[] keys, double period = 0.1, bool all = false, bool topicsOnly = false, bool prefix = false)
        {
            var full = new string[keys.Length];
            for (int i = 0; i < keys.Length; i++) full[i] = FullKey(keys[i]);
            return _source.Client.Subscribe(full, period, all, topicsOnly, prefix);
        }

        /// <summary>
        /// Subscribe using explicit options.
        /// </summary>
        public int Subscribe(string key, Nt4SubscriptionOptions opts)
        {
            return _source.Client.Subscribe(FullKey(key), opts);
        }

        /// <summary>
        /// Convenience read: get the latest value typed from the local cache in Nt4Source.
        /// </summary>
        public T? GetValue<T>(string key)
        {
            return _source.GetValue<T>(FullKey(key));
        }

        /// <summary>
        /// Convenience read at a timestamp from the local cache in Nt4Source.
        /// </summary>
        public T? GetValue<T>(string key, long timestampUs)
        {
            return _source.GetValue<T>(FullKey(key), timestampUs);
        }
    }
}
