using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver.Core.Compression;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.WireProtocol.Messages;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders.BinaryEncoders;
using MongoEmbedded;
using static MongoDB.Driver.Core.ConnectionPools.ExclusiveConnectionPool;
using static MongoDB.Driver.Core.Logging.LogCategories;

namespace MongoDB.Driver.Core.Connections
{
    /// <inheritdoc/>
    internal class EmbeddedConnection : IConnection
    {
        private static EmbeddedConnection Instance = null;

        /// <inheritdoc/>
        public ConnectionId ConnectionId { get; private set; }

        private readonly CompressorSource _compressorSource;

        /// <inheritdoc/>
        public ConnectionDescription Description { get; private set; }

        /// <inheritdoc/>
        public EndPoint EndPoint { get; private set; }

        /// <inheritdoc/>
        public int Generation => 0;

        /// <inheritdoc/>
        public bool IsExpired => false;

        /// <inheritdoc/>
        public ConnectionSettings Settings { get; private set; }

        private IConnectionInitializer _connectionInitializer;
        private ConnectionInitializerContext _connectionInitializerContext;

        private nint _client = 0;
        private nint _lib = 0;
        private nint _instance = 0;

        public EmbeddedConnection(
            EndPoint endPoint,
            ServerId serverId,
            ConnectionSettings settings,
            IConnectionInitializer connectionInitializer,
            IEventSubscriber eventSubscriber,
            ILoggerFactory loggerFactory)
        {


            Ensure.IsNotNull(serverId, nameof(serverId));
            EndPoint = Ensure.IsNotNull(endPoint, nameof(endPoint));
            Settings = Ensure.IsNotNull(settings, nameof(settings));
            _connectionInitializer = Ensure.IsNotNull(connectionInitializer, nameof(connectionInitializer));
            Ensure.IsNotNull(eventSubscriber, nameof(eventSubscriber));

            ConnectionId = new ConnectionId(serverId, settings.ConnectionIdLocalValueProvider());

            if (Instance != null)
                return;

            _compressorSource = new CompressorSource(settings.Compressors);
            Instance = this;
        }

        public unsafe void Open(OperationContext operationContext)
        {
            if (Instance != this)
            {
                Instance.Open(operationContext);
                Description = Instance.Description;
                return;
            }

            var status = MongoEmbeddedNative.mongo_embedded_v1_status_create();

            try
            {
                if (_lib == 0)
                {
                    var initParams = new MongoEmbeddedV1InitParams { LogFlags = MongoEmbeddedV1LogFlags.Stdout };
                    _lib = MongoEmbeddedNative.mongo_embedded_v1_lib_init(ref initParams, status);
                    if (_lib == 0)
                    {
                        var explain = MongoEmbeddedNative.mongo_embedded_v1_status_get_explanation(status);
                        throw new MongoConnectionException(ConnectionId, explain);
                    }
                }

                if (_instance == 0)
                {
                    var dbPath = Path.Combine(Environment.CurrentDirectory, "data");

                    if (!Directory.Exists(dbPath))
                        Directory.CreateDirectory(dbPath);

                    var config = new BsonDocument
                    {
                        { "storage", new BsonDocument
                            {
                                { "dbPath", dbPath },
                            }
                        }
                    }.ToJson();

                    _instance = MongoEmbeddedNative.mongo_embedded_v1_instance_create(_lib, config, status);
                    if (_instance == 0)
                    {
                        var explain = MongoEmbeddedNative.mongo_embedded_v1_status_get_explanation(status);
                        throw new MongoConnectionException(ConnectionId, explain);
                    }
                }

                if (_client == 0)
                {
                    _client = MongoEmbeddedNative.mongo_embedded_v1_client_create(_instance, status);
                    if (_client == 0)
                    {
                        var explain = MongoEmbeddedNative.mongo_embedded_v1_status_get_explanation(status);
                        throw new MongoConnectionException(ConnectionId, explain);
                    }

                    _connectionInitializerContext = _connectionInitializer.SendHello(operationContext, this);
                    Description = _connectionInitializerContext.Description;
                }
            }
            finally
            {
                MongoEmbeddedNative.mongo_embedded_v1_status_destroy(status);
            }
        }

        public Task OpenAsync(OperationContext operationContext)
        {
            Open(operationContext);
            return Task.CompletedTask;
        }

        public void Reauthenticate(OperationContext operationContext)
        {

        }

        public Task ReauthenticateAsync(OperationContext operationContext)
        {
            return Task.CompletedTask;
        }

        Queue<(byte[], int)> _responseQueue = new();

        public ResponseMessage ReceiveMessage(OperationContext operationContext, int responseTo, IMessageEncoderSelector encoderSelector, MessageEncoderSettings messageEncoderSettings)
        {
            if (Instance != this)
                return Instance.ReceiveMessage(operationContext, responseTo, encoderSelector, messageEncoderSettings);

            if (_responseQueue.Count == 0)
                return null;

            var (resp, size) = _responseQueue.Dequeue();

            var inputBufferChunkSource = new InputBufferChunkSource(BsonChunkPool.Default);
            using var buffer = ByteBufferFactory.Create(inputBufferChunkSource, size);
            buffer.SetBytes(0, resp, 0, size);
            buffer.MakeReadOnly();

            ArrayPool<byte>.Shared.Return(resp);

            var decoded = DecodeMessage(messageEncoderSettings, _compressorSource, operationContext, buffer, encoderSelector);
            return decoded;
        }

        public Task<ResponseMessage> ReceiveMessageAsync(OperationContext operationContext, int responseTo, IMessageEncoderSelector encoderSelector, MessageEncoderSettings messageEncoderSettings)
        {
            var result = ReceiveMessage(operationContext, responseTo, encoderSelector, messageEncoderSettings);
            return Task.FromResult(result);
        }

        public unsafe void SendMessage(OperationContext operationContext, RequestMessage message, MessageEncoderSettings messageEncoderSettings)
        {
            if (Instance != this)
            {
                Instance.SendMessage(operationContext, message, messageEncoderSettings);
                return;
            }

            Ensure.IsNotNull(message, nameof(message));
            operationContext.ThrowIfTimedOutOrCanceled();

            var status = MongoEmbeddedNative.mongo_embedded_v1_status_create();
            try
            {
                using var uncompressedBuffer = EncodeMessage(
                    message, messageEncoderSettings,
                    operationContext, out var sentMessage
                );

                var bytes = ArrayPool<byte>.Shared.Rent(uncompressedBuffer.Length);
                uncompressedBuffer.GetBytes(0, bytes, 0, uncompressedBuffer.Length);

                var error = MongoEmbeddedNative.mongo_embedded_v1_client_invoke(_client,
                    bytes, (ulong)uncompressedBuffer.Length,
                    out var resp,
                    out var respSize,
                    status);

                if (error != MongoEmbeddedV1Error.Success)
                {
                    var explain = MongoEmbeddedNative.mongo_embedded_v1_status_get_explanation(status);
                    throw new MongoConnectionException(ConnectionId, $"{error}: {explain}");
                }

                var respBuffer = ArrayPool<byte>.Shared.Rent((int)respSize);
                Marshal.Copy(resp, respBuffer, 0, (int)respSize);

                _responseQueue.Enqueue((respBuffer, (int)respSize));
            }
            finally
            {
                MongoEmbeddedNative.mongo_embedded_v1_status_destroy(status);
            }
        }

        public Task SendMessageAsync(OperationContext operationContext, RequestMessage message, MessageEncoderSettings messageEncoderSettings)
        {
            SendMessage(operationContext, message, messageEncoderSettings);
            return Task.CompletedTask;
        }

        public unsafe void Dispose()
        {
            if (Instance != this)
                return;

            if (_client != 0)
                MongoEmbeddedNative.mongo_embedded_v1_client_destroy(_client, null);
            if (_instance != 0)
                MongoEmbeddedNative.mongo_embedded_v1_instance_destroy(_instance, null);
            if (_lib != 0)
                MongoEmbeddedNative.mongo_embedded_v1_lib_fini(_lib, null);

            _responseQueue.Clear();
            Instance = null;
        }

        private IByteBuffer EncodeMessage(RequestMessage message, MessageEncoderSettings messageEncoderSettings, OperationContext operationContext, out RequestMessage sentMessage)
        {
            sentMessage = null;
            operationContext.ThrowIfTimedOutOrCanceled();

            var outputBufferChunkSource = new OutputBufferChunkSource(BsonChunkPool.Default);
            var buffer = new MultiChunkBuffer(outputBufferChunkSource);
            using (var stream = new ByteBufferStream(buffer, ownsBuffer: false))
            {
                var encoderFactory = new BinaryMessageEncoderFactory(stream, messageEncoderSettings, compressorSource: null);

                var encoder = message.GetEncoder(encoderFactory);
                encoder.WriteMessage(message);
                message.WasSent = true;
                sentMessage = message;

                // Encoding messages includes serializing the
                // documents, so encoding message could be expensive
                // and worthy of us honoring cancellation here.
                operationContext.ThrowIfTimedOutOrCanceled();

                buffer.Length = (int)stream.Length;
                buffer.MakeReadOnly();
            }

            return buffer;
        }

        private Opcode PeekOpcode(BsonStream stream)
        {
            var savedPosition = stream.Position;
            stream.Position += 12;
            var opcode = (Opcode)stream.ReadInt32();
            stream.Position = savedPosition;
            return opcode;
        }

        private ResponseMessage DecodeMessage(MessageEncoderSettings messageEncoderSettings, ICompressorSource compressorSource, OperationContext operationContext, IByteBuffer buffer, IMessageEncoderSelector encoderSelector)
        {
            operationContext.ThrowIfTimedOutOrCanceled();

            ResponseMessage message;
            using (var stream = new ByteBufferStream(buffer, ownsBuffer: false))
            {
                var encoderFactory = new BinaryMessageEncoderFactory(stream, messageEncoderSettings, compressorSource);

                var opcode = PeekOpcode(stream);
                if (opcode == Opcode.Compressed)
                {
                    var compresedMessageEncoder = encoderFactory.GetCompressedMessageEncoder(encoderSelector);
                    var compressedMessage = (CompressedMessage)compresedMessageEncoder.ReadMessage();
                    message = (ResponseMessage)compressedMessage.OriginalMessage;
                }
                else
                {
                    var encoder = encoderSelector.GetEncoder(encoderFactory);
                    message = (ResponseMessage)encoder.ReadMessage();
                }
            }

            return message;
        }
    }
}


namespace MongoEmbedded
{
    public enum MongoEmbeddedV1Error
    {
        ErrorInReportingError = -2,
        ErrorUnknown = -1,
        Success = 0,
        ErrorEnomem = 1,
        ErrorException = 2,
        ErrorLibraryAlreadyInitialized = 3,
        ErrorLibraryNotInitialized = 4,
        ErrorInvalidLibHandle = 5,
        ErrorDbInitializationFailed = 6,
        ErrorInvalidDbHandle = 7,
        ErrorHasDbHandlesOpen = 8,
        ErrorDbMaxOpen = 9,
        ErrorDbClientsOpen = 10,
        ErrorInvalidClientHandle = 11,
        ErrorReentrancyNotAllowed = 12
    }

    [Flags]
    public enum MongoEmbeddedV1LogFlags : ulong
    {
        None = 0,
        Stdout = 1,
        Callback = 4
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void MongoEmbeddedV1LogCallback(
        nint userData,
        [MarshalAs(UnmanagedType.LPStr)] string message,
        [MarshalAs(UnmanagedType.LPStr)] string component,
        [MarshalAs(UnmanagedType.LPStr)] string context,
        int severity
    );

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public struct MongoEmbeddedV1InitParams
    {
        [MarshalAs(UnmanagedType.LPStr)]
        public string YamlConfig;               // const char* yaml_config
        public MongoEmbeddedV1LogFlags LogFlags;                  // uint64_t log_flags
        public MongoEmbeddedV1LogCallback LogCallback; // mongo_embedded_v1_log_callback
        public nint LogUserData;              // void* log_user_data
    }

    public struct MongoEmbeddedV1Status { }
    public struct MongoEmbeddedV1Lib { }
    public struct MongoEmbeddedV1Instance { }
    public struct MongoEmbeddedV1Client { }

    public unsafe static class MongoEmbeddedNative
    {
        private const string DllName = "mongo_embedded.dll";

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern MongoEmbeddedV1Status* mongo_embedded_v1_status_create();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern void mongo_embedded_v1_status_destroy(MongoEmbeddedV1Status* status);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern MongoEmbeddedV1Error mongo_embedded_v1_status_get_error(MongoEmbeddedV1Status* status);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.LPStr)]
        internal static extern string mongo_embedded_v1_status_get_explanation(MongoEmbeddedV1Status* status);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int mongo_embedded_v1_status_get_code(MongoEmbeddedV1Status* status);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint mongo_embedded_v1_lib_init(
            ref MongoEmbeddedV1InitParams initParams,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern MongoEmbeddedV1Error mongo_embedded_v1_lib_fini(
            nint lib,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern nint mongo_embedded_v1_instance_create(
            nint lib,
            [MarshalAs(UnmanagedType.LPStr)] string yamlConfig,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern MongoEmbeddedV1Error mongo_embedded_v1_instance_destroy(
            nint instance,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint mongo_embedded_v1_client_create(
            nint instance,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern MongoEmbeddedV1Error mongo_embedded_v1_client_destroy(
            nint client,
            MongoEmbeddedV1Status* status
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern MongoEmbeddedV1Error mongo_embedded_v1_client_invoke(
            nint client,
            byte[] input,
            ulong inputSize,
            out nint output,
            out ulong outputSize,
            MongoEmbeddedV1Status* status
        );
    }
}

