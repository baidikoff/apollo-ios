#if !COCOAPODS
    import Apollo
#endif
import Foundation
import Starscream

// To allow for alternative implementations supporting the same WebSocketClient protocol
public class ApolloWebSocket: WebSocket, ApolloWebSocketClient {
    public required convenience init(request: URLRequest, protocols: [String]? = nil) {
        self.init(request: request, protocols: protocols, stream: FoundationStream())
    }
}

public protocol ApolloWebSocketClient: WebSocketClient {
    init(request: URLRequest, protocols: [String]?)
}

public protocol WebSocketTransportDelegate: class {
    func webSocketTransportDidConnect(_ webSocketTransport: WebSocketTransport)
    func webSocketTransportDidReconnect(_ webSocketTransport: WebSocketTransport)
    func webSocketTransport(_ webSocketTransport: WebSocketTransport, didDisconnectWithError error: Error?)
}

public extension WebSocketTransportDelegate {
    func webSocketTransportDidConnect(_ webSocketTransport: WebSocketTransport) {}
    func webSocketTransportDidReconnect(_ webSocketTransport: WebSocketTransport) {}
    func webSocketTransport(_ webSocketTransport: WebSocketTransport, didDisconnectWithError error: Error?) {}
}

/// A network transport that uses web sockets requests to send GraphQL subscription operations to a server, and that uses the Starscream implementation of web sockets.
public class WebSocketTransport {
    public static var provider: ApolloWebSocketClient.Type = ApolloWebSocket.self
    public weak var delegate: WebSocketTransportDelegate?

    var reconnect = false
    var websocket: ApolloWebSocketClient
    var error: Error?
    let serializationFormat = JSONSerializationFormat.self

    private final let protocols = ["graphql-ws"]

    private var acked = false

    private var queue: [Int: String] = [:]
    private var connectingPayload: GraphQLMap?

    private var subscribers = [String: (Result<JSONObject, Error>) -> Void]()
    private var subscriptions: [String: String] = [:]

    private let sendOperationIdentifiers: Bool
    private let reconnectionInterval: TimeInterval
    fileprivate var sequenceNumber = 0
    fileprivate var reconnected = false

    public init(request: URLRequest, sendOperationIdentifiers: Bool = false, reconnectionInterval: TimeInterval = 0.5, connectingPayload: GraphQLMap? = [:]) {
        self.connectingPayload = connectingPayload
        self.sendOperationIdentifiers = sendOperationIdentifiers
        self.reconnectionInterval = reconnectionInterval

        self.websocket = WebSocketTransport.provider.init(request: request, protocols: self.protocols)
        self.websocket.delegate = self
        self.websocket.connect()
    }

    public func isConnected() -> Bool {
        return self.websocket.isConnected
    }

    public func ping(data: Data, completionHandler: (() -> Void)? = nil) {
        return self.websocket.write(ping: data, completion: completionHandler)
    }

    private func processMessage(socket: WebSocketClient, text: String) {
        OperationMessage(serialized: text).parse { type, id, payload, error in
            guard
                let type = type,
                let messageType = OperationMessage.Types(rawValue: type) else {
                self.notifyErrorAllHandlers(WebSocketError(payload: payload, error: error, kind: .unprocessedMessage(text)))
                return
            }

            switch messageType {
            case .data, .error:
                if let id = id, let responseHandler = subscribers[id] {
                    if let payload = payload {
                        responseHandler(.success(payload))
                    } else if let error = error {
                        responseHandler(.failure(error))
                    } else {
                        let websocketError = WebSocketError(payload: payload,
                                                            error: error,
                                                            kind: .neitherErrorNorPayloadReceived)

                        responseHandler(.failure(websocketError))
                    }
                }
            case .complete:
                if let id = id {
                    // remove the callback if NOT a subscription
                    if subscriptions[id] == nil {
                        subscribers.removeValue(forKey: id)
                    }
                }

            case .connectionAck:
                acked = true
                writeQueue()

            case .connectionKeepAlive:
                writeQueue()

            case .connectionInit, .connectionTerminate, .start, .stop, .connectionError:
                if let id = id {
                    notifyErrorHandler(with: id, error: WebSocketError(payload: payload, error: error, kind: .unprocessedMessage(text)))
                } else {
                    notifyErrorAllHandlers(WebSocketError(payload: payload, error: error, kind: .unprocessedMessage(text)))
                }
            }
        }
    }

    private func notifyErrorHandler(with id: String, error: Error) {
        self.subscribers[id]?(.failure(error))
    }

    private func notifyErrorAllHandlers(_ error: Error) {
        for (_, handler) in self.subscribers {
            handler(.failure(error))
        }
    }

    private func writeQueue() {
        guard !self.queue.isEmpty else {
            return
        }

        let queue = self.queue.sorted(by: { $0.0 < $1.0 })
        self.queue.removeAll()
        for (id, msg) in queue {
            self.write(msg, id: id)
        }
    }

    private func processMessage(socket: WebSocketClient, data: Data) {
        print("WebSocketTransport::unprocessed event \(data)")
    }

    public func initServer(reconnect: Bool = true) {
        self.reconnect = reconnect
        self.acked = false

        if let str = OperationMessage(payload: self.connectingPayload, type: .connectionInit).rawMessage {
            self.write(str, force: true)
        }
    }

    public func closeConnection() {
        self.reconnect = false
        if let str = OperationMessage(type: .connectionTerminate).rawMessage {
            self.write(str)
        }
        self.queue.removeAll()
        self.subscriptions.removeAll()
    }

    private func write(_ str: String, force forced: Bool = false, id: Int? = nil) {
        if self.websocket.isConnected && (self.acked || forced) {
            self.websocket.write(string: str)
        } else {
            // using sequence number to make sure that the queue is processed correctly
            // either using the earlier assigned id or with the next higher key
            if let id = id {
                self.queue[id] = str
            } else if let id = queue.keys.max() {
                self.queue[id + 1] = str
            } else {
                self.queue[1] = str
            }
        }
    }

    deinit {
        websocket.disconnect()
        websocket.delegate = nil
    }

    fileprivate func nextSequenceNumber() -> Int {
        self.sequenceNumber += 1
        return self.sequenceNumber
    }

    fileprivate func sendHelper<Operation: GraphQLOperation>(operation: Operation, resultHandler: @escaping (_ result: Result<JSONObject, Error>) -> Void) -> String? {
        let body = RequestCreator.requestBody(for: operation, sendOperationIdentifiers: self.sendOperationIdentifiers)
        let sequenceNumber = "\(nextSequenceNumber())"

        guard let message = OperationMessage(payload: body, id: sequenceNumber).rawMessage else {
            return nil
        }

        self.write(message)

        self.subscribers[sequenceNumber] = resultHandler
        if operation.operationType == .subscription {
            self.subscriptions[sequenceNumber] = message
        }

        return sequenceNumber
    }

    public func unsubscribe(_ subscriptionId: String) {
        if let str = OperationMessage(id: subscriptionId, type: .stop).rawMessage {
            self.write(str)
        }
        self.subscribers.removeValue(forKey: subscriptionId)
        self.subscriptions.removeValue(forKey: subscriptionId)
    }

    fileprivate final class WebSocketTask<Operation: GraphQLOperation>: Cancellable {
        let sequenceNumber: String?
        let transport: WebSocketTransport

        init(_ ws: WebSocketTransport, _ operation: Operation, _ completionHandler: @escaping (_ result: Result<JSONObject, Error>) -> Void) {
            self.sequenceNumber = ws.sendHelper(operation: operation, resultHandler: completionHandler)
            self.transport = ws
        }

        public func cancel() {
            if let sequenceNumber = sequenceNumber {
                self.transport.unsubscribe(sequenceNumber)
            }
        }

        // unsubscribe same as cancel
        public func unsubscribe() {
            self.cancel()
        }
    }

    fileprivate final class OperationMessage {
        enum Types: String {
            case connectionInit = "connection_init" // Client -> Server
            case connectionTerminate = "connection_terminate" // Client -> Server
            case start // Client -> Server
            case stop // Client -> Server

            case connectionAck = "connection_ack" // Server -> Client
            case connectionError = "connection_error" // Server -> Client
            case connectionKeepAlive = "ka" // Server -> Client
            case data // Server -> Client
            case error // Server -> Client
            case complete // Server -> Client
        }

        let serializationFormat = JSONSerializationFormat.self
        var message: GraphQLMap = [:]
        var serialized: String?

        var rawMessage: String? {
            let serialized = try! self.serializationFormat.serialize(value: self.message)
            if let str = String(data: serialized, encoding: .utf8) {
                return str
            } else {
                return nil
            }
        }

        init(payload: GraphQLMap? = nil, id: String? = nil, type: Types = .start) {
            if let payload = payload {
                self.message += ["payload": payload]
            }
            if let id = id {
                self.message += ["id": id]
            }
            self.message += ["type": type.rawValue]
        }

        init(serialized: String) {
            self.serialized = serialized
        }

        func parse(handler: (_ type: String?, _ id: String?, _ payload: JSONObject?, _ error: Error?) -> Void) {
            guard let serialized = self.serialized else {
                handler(nil, nil, nil, WebSocketError(payload: nil, error: nil, kind: .serializedMessageError))
                return
            }

            guard let data = self.serialized?.data(using: .utf8) else {
                handler(nil, nil, nil, WebSocketError(payload: nil, error: nil, kind: .unprocessedMessage(serialized)))
                return
            }

            var type: String?
            var id: String?
            var payload: JSONObject?

            do {
                let json = try JSONSerializationFormat.deserialize(data: data) as? JSONObject

                id = json?["id"] as? String
                type = json?["type"] as? String
                payload = json?["payload"] as? JSONObject

                handler(type, id, payload, nil)
            } catch {
                handler(type, id, payload, WebSocketError(payload: payload, error: error, kind: .unprocessedMessage(serialized)))
            }
        }
    }
}

// MARK: - HTTPNetworkTransport conformance

extension WebSocketTransport: NetworkTransport {
    public func send<Operation>(operation: Operation, completionHandler: @escaping (_ result: Result<GraphQLResponse<Operation>, Error>) -> Void) -> Cancellable {
        if let error = self.error {
            completionHandler(.failure(error))
            return EmptyCancellable()
        }

        return WebSocketTask(self, operation) { result in
            switch result {
            case let .success(jsonBody):
                let response = GraphQLResponse(operation: operation, body: jsonBody)
                completionHandler(.success(response))
            case let .failure(error):
                completionHandler(.failure(error))
            }
        }
    }
}

// MARK: - WebSocketDelegate implementation

extension WebSocketTransport: WebSocketDelegate {
    public func websocketDidConnect(socket: WebSocketClient) {
        self.error = nil
        self.initServer()
        if self.reconnected {
            self.delegate?.webSocketTransportDidReconnect(self)
            // re-send the subscriptions whenever we are re-connected
            // for the first connect, any subscriptions are already in queue
            for (_, msg) in self.subscriptions {
                self.write(msg)
            }
        } else {
            self.delegate?.webSocketTransportDidConnect(self)
        }

        self.reconnected = true
    }

    public func websocketDidDisconnect(socket: WebSocketClient, error: Error?) {
        // report any error to all subscribers
        if let error = error {
            self.error = WebSocketError(payload: nil, error: error, kind: .networkError)
            self.notifyErrorAllHandlers(error)
        } else {
            self.error = nil
        }

        self.delegate?.webSocketTransport(self, didDisconnectWithError: self.error)
        self.acked = false // need new connect and ack before sending

        if self.reconnect {
            DispatchQueue.main.asyncAfter(deadline: .now() + self.reconnectionInterval) {
                self.websocket.connect()
            }
        }
    }

    public func websocketDidReceiveMessage(socket: WebSocketClient, text: String) {
        self.processMessage(socket: socket, text: text)
    }

    public func websocketDidReceiveData(socket: WebSocketClient, data: Data) {
        self.processMessage(socket: socket, data: data)
    }
}

public struct WebSocketError: Error, LocalizedError {
    public enum ErrorKind {
        case errorResponse
        case networkError
        case unprocessedMessage(String)
        case serializedMessageError
        case neitherErrorNorPayloadReceived

        var description: String {
            switch self {
            case .errorResponse:
                return "Received error response"
            case .networkError:
                return "Websocket network error"
            case let .unprocessedMessage(message):
                return "Websocket error: Unprocessed message \(message)"
            case .serializedMessageError:
                return "Websocket error: Serialized message not found"
            case .neitherErrorNorPayloadReceived:
                return "Websocket error: Did not receive an error or a payload."
            }
        }
    }

    /// The payload of the response.
    public let payload: JSONObject?
    public let error: Error?
    public let kind: ErrorKind

    public var errorDescription: String? {
        return "\(self.kind.description). Error: \(String(describing: self.error))"
    }
}
