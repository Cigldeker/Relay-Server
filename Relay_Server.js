//var SERVER_HOST='127.0.0.1';
    var SERVER_PORT = 10167;
    var PACKET_ID = 0xABCDEF01;

    var STATUS_SUCCESS = 0;
    var STATUS_FAILURE = 1;

    var UPDATE_GATEWAY      = 1;
    var UPDATE_GATEWAY_RESP = 2;
    var HOLE_PUNCHING       = 3;
    var HOLE_PUNCHING_RESP  = 4;
    var HOLE_PUNCHING_INIT  = 5;

    var ZNP_MSG = 0x0000FFF3;


    var CLEAN_GATEWAYS_PERIOD  = 10;
    var AGING_PERIOD = 3600;  // 1 hour
    var cleanGatewaysCnt = CLEAN_GATEWAYS_PERIOD;

    var ip = require('ip');
	
    var dgram = require('dgram');
    var udpServer = dgram.createSocket("udp4");

    var dataStore = require('nedb');
    var gwStore = new dataStore({filename:'gwStore.db', autoload: true});
    gwStore.persistence.setAutocompactionInterval(20000);

    var udpMsg = new Buffer(200);

	process.on(
		'uncaughtException',
		function(err) {
			console.error(err);
		}
	);

    udpServer.on(
        "listening",
        function () {
            var serverAddress = udpServer.address();
            console.log("UDP Server listening at: " +
                        serverAddress.address +
                        ":" +
                        serverAddress.port);
        }
    );

    udpServer.on(
        "error",
        function (err) {
            console.log('udpServer Error: ' + err);
            udpServer.close();
        }
    );

    udpServer.on(
        "message",
        function (message, sender) {

            var serialNo = 0;
            var ipLong;

            var msgIdx = 0;
            var rspIdx = 0;

            var currDate = new Date();

            var packetID = message.readUInt32LE(msgIdx);
            msgIdx += 4;
            if(packetID != PACKET_ID){
                console.log("bad Packet ID " +
                            sender.address +
                            ":" +
                            sender.port);
                return;
            }

            var packetFunc = message.readUInt32LE(msgIdx);
            msgIdx += 4;

            var destInfo = {};
            ipLong = message.readUInt32LE(msgIdx);
            destInfo.address = ip.fromLong(ipLong);
            msgIdx += 4;
            destInfo.port = message.readUInt16LE(msgIdx);
            msgIdx += 2;

            var msgSeqNo = message.readUInt8(msgIdx);
            msgIdx++;

            switch (packetFunc){

                case UPDATE_GATEWAY:

                    serialNo = message.readUInt32LE(msgIdx);
                    msgIdx += 4;
                    var logTime = parseInt(currDate.getTime() / 1000);

                    var secFrom2000 = parseInt(Date.UTC(2000, 0, 1) / 1000);
                    var zbTime = parseInt(logTime - secFrom2000);

                    gwStore.update(
                        { serial_No: serialNo }, // query
                        { $set: {
                            serial_No: serialNo,
                            public_IP: sender.address,
                            public_Port: sender.port,
                            log_Time:logTime }
                        },
                        { upsert:true },
                        function(err) {
                            rspIdx = 0;
                            udpMsg.writeUInt32LE(PACKET_ID, rspIdx);
                            rspIdx += 4;
                            udpMsg.writeUInt32LE(UPDATE_GATEWAY_RESP, rspIdx);
                            rspIdx += 4;

                            ipLong = ip.toLong(sender.address);
                            udpMsg.writeUInt32LE(ipLong, rspIdx);
                            rspIdx += 4;
                            udpMsg.writeUInt16LE(sender.port, rspIdx);
                            rspIdx += 2;
                            udpMsg.writeUInt8(msgSeqNo, rspIdx);
                            rspIdx++;

                            var status = STATUS_SUCCESS;
                            if(err) {
                                status = STATUS_FAILURE;
                            }
                            udpMsg.writeUInt16LE(status, rspIdx);
                            rspIdx += 2;

                            udpMsg.writeUInt32LE(zbTime, rspIdx);
                            rspIdx += 4;

                            var updateResp = new Buffer(rspIdx);
                            udpMsg.copy(updateResp, 0, 0, rspIdx);

                            udpServer.send(
                                updateResp,
                                0,
                                updateResp.length,
                                sender.port,
                                sender.address,
                                function(){
                                    console.log(currDate.toLocaleTimeString() +
                                                ": Update Gateway Response to: "+
                                                sender.address +
                                                ":" +
                                                sender.port);
                                }
                            );
                        }
                    );
                    break;

                case HOLE_PUNCHING:

                    serialNo = message.readUInt32LE(msgIdx);
                    msgIdx += 4;

                    rspIdx = 0;
                    udpMsg.writeUInt32LE(PACKET_ID, rspIdx);
                    rspIdx += 4;
                    udpMsg.writeUInt32LE(HOLE_PUNCHING_RESP, rspIdx);
                    rspIdx += 4;

                    ipLong = ip.toLong(sender.address);
                    udpMsg.writeUInt32LE(ipLong, rspIdx);
                    rspIdx += 4;
                    udpMsg.writeUInt16LE(sender.port, rspIdx);
                    rspIdx += 2;
                    udpMsg.writeUInt8(msgSeqNo, rspIdx);
                    rspIdx++;
					
                    gwStore.findOne(
                        { serial_No : serialNo },
                        function(err, gwRecord){
                            if(gwRecord){
                                udpMsg.writeUInt16LE(STATUS_SUCCESS, rspIdx);
                                rspIdx += 2;
                                ipLong = ip.toLong(gwRecord.public_IP);
                                udpMsg.writeUInt32LE(ipLong, rspIdx);
                                rspIdx += 4;
                                udpMsg.writeUInt16LE(gwRecord.public_Port, rspIdx);
                                rspIdx += 2;

                                var hpResMsg = new Buffer(rspIdx);
                                udpMsg.copy(hpResMsg, 0, 0, rspIdx);

                                udpServer.send(
                                    hpResMsg,
                                    0,
                                    hpResMsg.length,
                                    sender.port,
                                    sender.address,
                                    function(){
                                        console.log("HP response to initiator at: "+
                                                    sender.address + ":" + sender.port);

                                        var gwMsgIdx = 0;
                                        udpMsg.writeUInt32LE(PACKET_ID, gwMsgIdx);
                                        gwMsgIdx += 4;
                                        udpMsg.writeUInt32LE(HOLE_PUNCHING_INIT, gwMsgIdx);
                                        gwMsgIdx += 4;

                                        ipLong = ip.toLong(sender.address);
                                        udpMsg.writeUInt32LE(ipLong, gwMsgIdx);
                                        gwMsgIdx += 4;
                                        udpMsg.writeUInt16LE(sender.port, gwMsgIdx);
                                        gwMsgIdx += 2;
                                        udpMsg.writeUInt8(msgSeqNo, gwMsgIdx);
                                        gwMsgIdx++;

                                        var hpInitMsg = new Buffer(gwMsgIdx);
                                        udpMsg.copy(hpInitMsg, 0, 0, gwMsgIdx);

                                        udpServer.send(
                                            hpInitMsg,
                                            0,
                                            hpInitMsg.length,
                                            gwRecord.public_Port,
                                            gwRecord.public_IP,
                                            function(){
                                                console.log("HP init to gateway at: "+
                                                gwRecord.public_IP + ":" + gwRecord.public_Port);
                                            }
                                        );
                                    }
                                );
                            }
                            else {
                                udpMsg.writeUInt16LE(STATUS_FAILURE, rspIdx);
                                rspIdx += 2;

                                var failureMsg = new Buffer(rspIdx);
                                udpMsg.copy(failureMsg, 0, 0, rspIdx);

                                udpServer.send(
                                    failureMsg,
                                    0,
                                    failureMsg.length,
                                    sender.port,
                                    sender.address,
                                    function(){
                                        console.log("HP failed "+
                                                    sender.address + ":" + sender.port);
                                    }
                                );
                            }
                        }
                    );

                    break;

                case ZNP_MSG:

                    msgIdx = 4 + 4; // packet ID + packet function
                    ipLong = ip.toLong(sender.address);
                    message.writeUInt32LE(ipLong, msgIdx);
                    msgIdx += 4;
                    message.writeUInt16LE(sender.port, msgIdx);
                    msgIdx += 2;

                    udpServer.send(
                        message,
                        0,
                        message.length,
                        destInfo.port,
                        destInfo.address,
                        function(){
                            console.log(currDate.toLocaleTimeString() +
                                        ": znp msg from " +
                                        sender.address + ":" + sender.port + ' cmd: ' +
                                        parseInt(message.readUInt8(16)) + ' id: ' +
                                        parseInt(message.readUInt8(17)));
                        }
                    );

                    break;

            }
        }
    );

    setTimeout(cleanGateways, 1000);

    function cleanGateways(){
        cleanGatewaysCnt--;
        if(cleanGatewaysCnt == 0){
            cleanGatewaysCnt = CLEAN_GATEWAYS_PERIOD;
            var currentTime = new Date().getTime() / 1000;
            var agedTime = currentTime - AGING_PERIOD;

            gwStore.remove(
				{ log_Time: { $lte:agedTime } },
				{},
				function() {
					//
				}
			);
        }
        setTimeout(cleanGateways, 1000);
    }



    udpServer.bind(SERVER_PORT);
