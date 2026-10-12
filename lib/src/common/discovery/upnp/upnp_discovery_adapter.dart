import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;

import '../../adapter/raw_datagram_socket_adapter.dart';
import 'upnp_advertisement_message.dart';
import 'upnp_device_description.dart';
import 'upnp_device_description_parser.dart';

abstract class MessageType {
  const MessageType._();

  static const notify = 'NOTIFY * HTTP/1.1';
  static const mSearchResponse = 'HTTP/1.1 200 OK';
}

abstract class NotificationSubType {
  const NotificationSubType._();

  static const alive = 'ssdp:alive';
  static const byeBye = 'ssdp:byebye';
}

class UpnpDiscoveryAdapter {
  static const upnpPort = 1900;
  final ipv4MulticastAddress = InternetAddress('239.255.255.250');
  final ipv4BroadcastAddress = InternetAddress('255.255.255.255');

  /// Service type used for active M-SEARCH queries. Same Canon PTP/IP
  /// service that passive NOTIFY listening filtered for.
  static const mSearchServiceType =
      'urn:schemas-canon-com:service:ICPO-WFTEOSSystemService:1';

  final RawDatagramSocketFactory _socketFactory;
  final UpnpDeviceDescriptionParser _deviceDescriptionParser;

  UpnpDiscoveryAdapter({
    RawDatagramSocketFactory? socketFactory,
    UpnpDeviceDescriptionParser? deviceDescriptionParser,
  })  : _socketFactory = socketFactory ?? const RawDatagramSocketFactory(),
        _deviceDescriptionParser =
            deviceDescriptionParser ?? const UpnpDeviceDescriptionParser();

  /// Active UPnP discovery via M-SEARCH.
  ///
  /// The previous implementation passively listened for NOTIFY
  /// advertisements by joining the SSDP multicast group. Joining a
  /// multicast group on iOS requires the
  /// com.apple.developer.networking.multicast entitlement, which Apple
  /// must approve per app and which is unavailable to sideloaded apps,
  /// so discovery never received anything on iOS.
  ///
  /// M-SEARCH instead _sends_ a query (multicast + limited broadcast)
  /// and receives the unicast responses on an ephemeral port, which
  /// needs no special entitlement.
  Stream<UpnpAdvertisementMessage> discover() {
    final controller = StreamController<UpnpAdvertisementMessage>();

    controller.onListen = () async {
      // Ephemeral port: we only need the unicast M-SEARCH responses.
      final socket = await _socketFactory.bind(InternetAddress.anyIPv4, 0);
      Timer? searchTimer;

      controller.onCancel = () {
        searchTimer?.cancel();
        socket.close();
      };

      Future<void> sendMSearch() async {
        final message = 'M-SEARCH * HTTP/1.1\r\n'
            'HOST: 239.255.255.250:1900\r\n'
            'MAN: "ns=01"\r\n'
            'ST: $mSearchServiceType\r\n'
            'MX: 3\r\n'
            '\r\n';
        final data = utf8.encode(message);
        // Multicast per UPnP spec, plus limited broadcast as fallback.
        socket.send(data, ipv4MulticastAddress, upnpPort);
        socket.send(data, ipv4BroadcastAddress, upnpPort);
      }

      // Re-query periodically so cameras appearing later are found.
      searchTimer =
          Timer.periodic(const Duration(seconds: 4), (_) => sendMSearch());
      await sendMSearch();

      controller.addStream(
        socket.stream.transform(
          StreamTransformer.fromHandlers(
            handleData: (event, sink) {
              if (event != RawSocketEvent.read) {
                return;
              }

              final datagram = socket.receive();
              if (datagram == null) {
                return;
              }

              final message = _processMessage(datagram);
              if (message != null) {
                sink.add(message);
              }
            },
          ),
        ),
      );
    };

    return controller.stream;
  }

  Future<UpnpDeviceDescription?> getDeviceDescription(String location) async {
    final response = await http.get(Uri.parse(location));

    return _deviceDescriptionParser.parse(location, response.body);
  }

  UpnpAdvertisementMessage? _processMessage(Datagram datagram) {
    final payload = utf8.decode(datagram.data);

    final lines = payload.split('\n');
    final messageHeader = lines.removeAt(0).toUpperCase().trim();
    final entries = _parseEntries(lines);

    if (messageHeader == MessageType.notify) {
      return _mapNotifyToMessage(entries);
    } else if (messageHeader == MessageType.mSearchResponse) {
      return _mapMSearchResponseToMessage(entries);
    }

    return null;
  }

  Map<String, String> _parseEntries(List<String> lines) {
    final entryRegex = RegExp(r'\s*([\w-]*):\s*(.*)');
    final entries = <String, String>{};

    for (final line in lines) {
      final entryMatch = entryRegex.firstMatch(line);
      if (entryMatch == null || entryMatch.groupCount != 2) {
        continue;
      }

      final key = entryMatch.group(1)!.toUpperCase();
      final value = entryMatch.group(2)!;
      entries[key] = value;
    }

    return entries;
  }

  UpnpAdvertisementMessage? _mapNotifyToMessage(Map<String, String> entries) {
    try {
      final notifcationSubType = entries['NTS'];
      switch (notifcationSubType) {
        case NotificationSubType.alive:
          {
            return UpnpAdvertisementMessage.alive(
              serviceType: entries['NT']!,
              uniqueServiceName: entries['USN']!,
              location: entries['LOCATION']!,
            );
          }
        case NotificationSubType.byeBye:
          {
            return UpnpAdvertisementMessage.byeBye(
              serviceType: entries['NT']!,
              uniqueServiceName: entries['USN']!,
            );
          }
      }
    } catch (e) {
      return null;
    }

    return null;
  }

  UpnpAdvertisementMessage? _mapMSearchResponseToMessage(
      Map<String, String> entries) {
    try {
      return UpnpAdvertisementMessage.alive(
        serviceType: entries['ST']!,
        uniqueServiceName: entries['USN']!,
        location: entries['LOCATION']!,
      );
    } catch (e) {
      return null;
    }
  }
}
