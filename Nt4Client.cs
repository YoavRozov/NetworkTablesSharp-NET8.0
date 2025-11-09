using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace NetworkTablesSharp
{
    public class Nt4Client : IAsyncDisposable
    {
        public static readonly Dictionary<string, int> TypeStrIdxLookup = new()
        {
            { "boolean", 0 },
            { "double", 1 },
            { "int", 2 },
            { "string", 3 },
            { "json", 4 },
            { "raw", 5 },
            { "rpc", 5 },
            { "msgpack", 5 },
            { "protobuf", 5 },
            { "boolean[]", 16 },
            { "double[]", 17 },
            { "int[]", 18 },
            { "float[]", 19 },
            { "string[]", 20 },
        };

        private readonly string _serverAddress;
        private readonly EventHandler? _onOpen;
        private readonly Action<Nt4Topic, long, object>? _onNewTopicData;
        private readonly string _appName;

        // Transport state
        private ClientWebSocket? _ws;
        private readonly CancellationTokenSource _cts = new();

        // Time sync
        private long? _serverTimeOffsetUs;
        private long _networkLatencyUs;

        // NT4 state
        private readonly Dictionary<int, Nt4Subscription> _subscriptions = new();
        private readonly Dictionary<string, Nt4Topic> _publishedTopics = new();
        private readonly Dictionary<string, Nt4Topic> _serverTopics = new();

        public Nt4Client(string appName, string serverBaseAddress, int serverPort = 5810,
                         EventHandler? onOpen = null,
                         Action<Nt4Topic, long, object>? onNewTopicData = null)
        {
            // Same URI as original: ws://<ip>:<port>/nt/<appName>
            _serverAddress = "ws://" + serverBaseAddress + ":" + serverPort + "/nt/" + appName;
            _onOpen = onOpen;
            _onNewTopicData = onNewTopicData;
            _appName = appName;
        }

        /// <summary>Connect to the NetworkTables server (async).</summary>
        public async Task<(bool success, string? errorMessage)> ConnectAsync()
        {
            try
            {
                if (_ws is { State: WebSocketState.Open or WebSocketState.Connecting })
                    return (true, null);

                _ws = new ClientWebSocket();
                await _ws.ConnectAsync(new Uri(_serverAddress), _cts.Token).ConfigureAwait(false);

                Console.WriteLine("[NT4] Connected with identity " + _appName);
                _onOpen?.Invoke(this, EventArgs.Empty);

                // Send initial timestamp (handshake/time sync)
                await WsSendTimestampAsync().ConfigureAwait(false);

                // Start receive loop
                _ = Task.Run(ReceiveLoop, _cts.Token);

                return (true, null);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }
        }

        /// <summary>Legacy sync wrapper (keeps Nt4Source.cs working as-is).</summary>
        public (bool success, string? errorMessage) Connect()
        {
            var t = ConnectAsync();
            t.GetAwaiter().GetResult();
            return t.Result;
        }

        /// <summary>Disconnect from the NetworkTables server.</summary>
        public async Task DisconnectAsync()
        {
            if (!Connected()) return;
            try
            {
                _cts.Cancel();
                if (_ws is { State: WebSocketState.Open or WebSocketState.CloseReceived })
                {
                    await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "client disconnect", CancellationToken.None)
                             .ConfigureAwait(false);
                }
            }
            catch { /* ignore */ }
            finally
            {
                _serverTopics.Clear();
                _publishedTopics.Clear();
                _subscriptions.Clear();
            }
        }

        public void Disconnect() => DisconnectAsync().GetAwaiter().GetResult();

        private async Task ReceiveLoop()
        {
            var buffer = new byte[64 * 1024];

            try
            {
                while (!_cts.IsCancellationRequested && _ws is { State: WebSocketState.Open })
                {
                    var segment = new ArraySegment<byte>(buffer);
                    var result = await _ws.ReceiveAsync(segment, _cts.Token).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        Console.WriteLine("[NT4] Disconnected: remote close");
                        break;
                    }

                    if (!result.EndOfMessage)
                    {
                        // NOTE: For simplicity, assuming messages fit in 64KB.
                        // If needed, accumulate until EndOfMessage == true.
                    }

                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                        // Original code expects a JSON array of one or more objects.
                        var arr = JsonConvert.DeserializeObject<object[]>(json);
                        if (arr == null)
                        {
                            Console.WriteLine("[NT4] Failed to decode JSON message.");
                            continue;
                        }
                        foreach (var obj in arr)
                        {
                            var objStr = obj?.ToString();
                            if (objStr == null) continue;

                            var msgObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(objStr);
                            if (msgObj == null)
                            {
                                Console.WriteLine("[NT4] Failed to decode JSON message object.");
                                continue;
                            }
                            HandleJsonMessage(msgObj);
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        // MsgPack array: [topicId:int, timestamp:long, typeIdx:int, value:any]
                        var slice = new ReadOnlyMemory<byte>(buffer, 0, result.Count);
                        var msg = MessagePackSerializer.Deserialize<object[]>(slice);
                        if (msg == null)
                        {
                            Console.WriteLine("[NT4] Failed to decode MSGPack message.");
                            continue;
                        }
                        HandleMsgPackMessage(msg);
                    }
                }
            }
            catch (OperationCanceledException) { /* normal on shutdown */ }
            catch (Exception ex)
            {
                Console.WriteLine("[NT4] Error in receive loop: " + ex);
            }
            finally
            {
                _serverTopics.Clear();
                _publishedTopics.Clear();
                _subscriptions.Clear();
            }
        }

        // === Public API (unchanged behavior) ===

        public void PublishTopic(string key, string type)
            => PublishTopic(key, type, new Dictionary<string, object>());

        public void PublishTopic(string key, string type, Dictionary<string, object> properties)
        {
            var topic = new Nt4Topic(GetNewUid(), key, type, properties);
            if (!Connected() || _publishedTopics.ContainsKey(key)) return;
            _publishedTopics.Add(key, topic);
            _ = WsPublishTopicAsync(topic);
        }

        public void UnpublishTopic(string key)
        {
            if (!Connected()) return;
            if (!_publishedTopics.TryGetValue(key, out var topic))
            {
                Console.WriteLine("[NT4] Attempted to unpublish topic that was not published: " + key);
                return;
            }
            _publishedTopics.Remove(key);
            _ = WsUnpublishTopicAsync(topic);
        }

        public void PublishValue(string key, object value)
        {
            long timestamp = GetServerTimeUs() ?? 0;
            if (!Connected()) return;
            if (!_publishedTopics.TryGetValue(key, out var topic))
            {
                Console.WriteLine("[NT4] Attempted to publish value for topic that was not published: " + key);
                return;
            }
            var payload = MessagePackSerializer.Serialize(new object[] { topic.Uid, timestamp, TypeStrIdxLookup[topic.Type], value });
            _ = WsSendBinaryAsync(payload);
        }

        public int Subscribe(string key, double periodic = 0.1, bool all = false, bool topicsOnly = false, bool prefix = false)
        {
            if (!Connected()) return -1;
            var opts = new Nt4SubscriptionOptions(periodic, all, topicsOnly, prefix);
            var sub = new Nt4Subscription(GetNewUid(), new[] { key }, opts);
            _ = WsSubscribeAsync(sub);
            _subscriptions.Add(sub.Uid, sub);
            return sub.Uid;
        }

        public int Subscribe(string key, Nt4SubscriptionOptions opts)
        {
            if (!Connected()) return -1;
            var sub = new Nt4Subscription(GetNewUid(), new[] { key }, opts);
            _ = WsSubscribeAsync(sub);
            _subscriptions.Add(sub.Uid, sub);
            return sub.Uid;
        }

        public int Subscribe(string[] keys, double periodic = 0.1, bool all = false, bool topicsOnly = false, bool prefix = false)
        {
            if (!Connected()) return -1;
            var opts = new Nt4SubscriptionOptions(periodic, all, topicsOnly, prefix);
            var sub = new Nt4Subscription(GetNewUid(), keys, opts);
            _ = WsSubscribeAsync(sub);
            _subscriptions.Add(sub.Uid, sub);
            return sub.Uid;
        }

        public void Unsubscribe(int uid)
        {
            if (!Connected()) return;
            if (!_subscriptions.TryGetValue(uid, out var sub))
            {
                Console.WriteLine("[NT4] Attempted to unsubscribe from a subscription that does not exist: " + uid);
                return;
            }
            _subscriptions.Remove(uid);
            _ = WsUnsubscribeAsync(sub);
        }

        public long? GetServerTimeUs()
        {
            if (_serverTimeOffsetUs == null) return null;
            return GetClientTimeUs() + _serverTimeOffsetUs;
        }

        public bool Connected() => _ws is { State: WebSocketState.Open };

        // === Transport helpers (async) ===

        private async Task WsSendJsonAsync(string method, Dictionary<string, object> @params)
        {
            if (!Connected()) return;

            var msg = new Dictionary<string, object>
            {
                { "method", method },
                { "params", @params }
            };

            var jsonArray = JsonConvert.SerializeObject(new object[] { msg });
            var bytes = Encoding.UTF8.GetBytes(jsonArray);
            await _ws!.SendAsync(bytes, WebSocketMessageType.Text, true, _cts.Token).ConfigureAwait(false);
        }

        private Task WsPublishTopicAsync(Nt4Topic topic)
            => WsSendJsonAsync("publish", topic.ToPublishObj());

        private Task WsUnpublishTopicAsync(Nt4Topic topic)
            => WsSendJsonAsync("unpublish", topic.ToUnpublishObj());

        private Task WsSubscribeAsync(Nt4Subscription subscription)
            => WsSendJsonAsync("subscribe", subscription.ToSubscribeObj());

        private Task WsUnsubscribeAsync(Nt4Subscription subscription)
            => WsSendJsonAsync("unsubscribe", subscription.ToUnsubscribeObj());

        private async Task WsSendBinaryAsync(byte[] data)
        {
            if (!Connected())
            {
                Console.WriteLine("[NT4] Attempted to send data while the WebSocket was not open");
                return;
            }
            await _ws!.SendAsync(data, WebSocketMessageType.Binary, true, _cts.Token).ConfigureAwait(false);
        }

        private async Task WsSendTimestampAsync()
        {
            long timestamp = GetClientTimeUs();
            // [-1, 0, type(int), timestamp]
            var payload = MessagePackSerializer.Serialize(new object[] { -1, 0, TypeStrIdxLookup["int"], timestamp });
            await WsSendBinaryAsync(payload).ConfigureAwait(false);
        }

        // === Message handlers (same logic as original) ===

        private void HandleJsonMessage(Dictionary<string, object> msg)
        {
            if (!msg.ContainsKey("method") || !msg.ContainsKey("params")) return;
            if (!(msg["method"] is string method)) return;
            if (!(msg["params"] is JObject parameters)) return;

            var parametersDict = parameters.ToObject<Dictionary<string, object>>();
            if (parametersDict == null)
            {
                Console.WriteLine("[NT4] Failed to decode JSON parameters: " + parameters);
                return;
            }

            if (method == "announce")
            {
                var topic = new Nt4Topic(parametersDict);
                if (_serverTopics.ContainsKey(topic.Name))
                {
                    Console.WriteLine("[NT4] Received announcement for topic that already exists: " + topic.Name);
                    _serverTopics.Remove(topic.Name);
                }
                _serverTopics.Add(topic.Name, topic);
            }
            else if (method == "unannounce")
            {
                if (parametersDict["name"] is string name)
                {
                    if (!_serverTopics.ContainsKey(name))
                    {
                        Console.WriteLine("[NT4] Received unannounce for topic that does not exist: " + name);
                        return;
                    }
                    _serverTopics.Remove(name);
                }
            }
            else if (method == "properties")
            {
                if (!(parametersDict["name"] is string name)) return;
                if (!_serverTopics.ContainsKey(name))
                {
                    Console.WriteLine("[NT4] Received properties update for topic that does not exist: " + name);
                    return;
                }

                var topic = _serverTopics[name];
                foreach (var entry in (Dictionary<string, object>)parametersDict["update"])
                {
                    if (entry.Value == null) topic.RemoveProperty(entry.Key);
                    else topic.SetProperty(entry.Key, entry.Value);
                }
            }
        }

        private void HandleMsgPackMessage(object[] msg)
        {
            int topicId = Convert.ToInt32(msg[0]);
            long timestampUs = Convert.ToInt64(msg[1]);
            object value = msg[3];

            if (topicId >= 0)
            {
                Nt4Topic? topic = null;
                foreach (var serverTopic in _serverTopics.Values)
                {
                    if (serverTopic.Uid == topicId) { topic = serverTopic; break; }
                }
                if (topic == null) return;

                _onNewTopicData?.Invoke(topic, timestampUs, value);
            }
            else if (topicId == -1)
            {
                WsHandleReceiveTimestamp(timestampUs, Convert.ToInt64(value));
            }
        }

        // === Time utils ===

        private static long GetClientTimeUs()
        {
            long ms = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalMilliseconds;
            return ms * 1000;
        }

        private void WsHandleReceiveTimestamp(long serverTimestamp, long clientTimestamp)
        {
            long rxTime = GetClientTimeUs();
            long rtt = rxTime - clientTimestamp;
            _networkLatencyUs = rtt / 2L;
            long serverTimeAtRx = serverTimestamp + _networkLatencyUs;
            _serverTimeOffsetUs = serverTimeAtRx - rxTime;

            Console.WriteLine($"[NT4] New server time: {(GetServerTimeUs() / 1_000_000.0)}s with {(_networkLatencyUs / 1000.0)}ms latency");
        }

        private static int GetNewUid() => new Random().Next(0, 10000000);

        public async ValueTask DisposeAsync()
        {
            await DisconnectAsync();
            _ws?.Dispose();
            _cts.Dispose();
        }
    }
}
