//var SERVER_HOST='127.0.0.1';
    var SERVER_PORT = 10167;
    var PACKET_ID = 0xABCDEF01;

    var MCAST_SERVER_IP = '228.1.9.70';
    var MCAST_SERVER_PORT = 26544;

    var STATUS_SUCCESS = 0;
    var STATUS_FAILURE = 1;

    var UPDATE_GATEWAY      = 1;
    var UPDATE_GATEWAY_RESP = 2;
    var HOLE_PUNCHING       = 3;
    var HOLE_PUNCHING_RESP  = 4;
    var HOLE_PUNCHING_INIT  = 5;

    var MCAST_TEST = 101;

    var ZNP_MSG = 0x0000FFF3;
    var OTA_MSG = 0x0000FFF4;

    var MT_OTA_FILE_READ_REQ = 0x00;
    var MT_OTA_NEXT_IMG_REQ = 0x01;
    var MT_OTA_FILE_READ_RSP = 0x80;
    var MT_OTA_NEXT_IMG_RSP = 0x81;

    var ZCL_STATUS_SUCCESS = 0x00;
    //var ZCL_STATUS_WAIT_FOR_DATA = 0x97;
    var ZCL_STATUS_ABORT = 0x95;
    var ZSuccess = 0x00;
    var ZOtaNoImageAvailable = 0x98;

    var Addr16Bit = 2;
    //var Addr64Bit = 3;
    //var AddrBroadcast = 15;

    var CLEAN_GATEWAYS_PERIOD  = 10;
    var AGING_PERIOD = 3600;  // 1 hour
    var cleanGatewaysCnt = CLEAN_GATEWAYS_PERIOD;

    var ip = require('ip');
    var path = require('path');
    var fs = require('fs');
	
    var dgram = require('dgram');
    var udpServer = dgram.createSocket("udp4");

    var mcastServer = dgram.createSocket("udp4");

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

            msgIdx += 4; // skip src address
            msgIdx += 2; // skip src port

            var msgSeqNo = message.readUInt8(msgIdx);
            msgIdx++;

            switch (packetFunc){

                case UPDATE_GATEWAY: // --------------------------------------------------------------------------------

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
                            udpMsg.copy(updateResp,
                                        0,
                                        0,
                                        rspIdx);

                            udpServer.send(updateResp,
                                           0,
                                           updateResp.length,
                                           sender.port,
                                           sender.address,
                                           function (){
                                               console.log(currDate.toLocaleTimeString() +
                                                           ": Update Gateway Response to: " +
                                                           sender.address +
                                                           ":" +
                                                           sender.port);
                                           }
                            );
                        }
                    );
                    break;

                case HOLE_PUNCHING: // ---------------------------------------------------------------------------------

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

                                udpServer.send(hpResMsg,
                                               0,
                                               hpResMsg.length,
                                               sender.port,
                                               sender.address,
                                               function (){
                                                   console.log("HP response to initiator at: " +
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
                                                   udpMsg.copy(hpInitMsg,
                                                               0,
                                                               0,
                                                               gwMsgIdx);

                                                   udpServer.send(hpInitMsg,
                                                                  0,
                                                                  hpInitMsg.length,
                                                                  gwRecord.public_Port,
                                                                  gwRecord.public_IP,
                                                                  function (){
                                                                      console.log("HP init to gateway at: " +
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
                                udpMsg.copy(failureMsg,
                                            0,
                                            0,
                                            rspIdx);

                                udpServer.send(failureMsg,
                                               0,
                                               failureMsg.length,
                                               sender.port,
                                               sender.address,
                                               function (){
                                                   console.log("HP failed " +
                                                               sender.address + ":" + sender.port);
                                               }
                                );
                            }
                        }
                    );

                    break;

                case ZNP_MSG: // ---------------------------------------------------------------------------------------

                    msgIdx = 4 + 4; // packet ID + packet function
                    // get destination address
                    ipLong = message.readUInt32LE(msgIdx);
                    var destAddress = ip.fromLong(ipLong);
                    msgIdx += 4;
                    var destPort = message.readUInt16LE(msgIdx);

                    msgIdx = 4 + 4; // "rewind" index
                    // overwrite msg source address
                    ipLong = ip.toLong(sender.address);
                    message.writeUInt32LE(ipLong, msgIdx);
                    msgIdx += 4;
                    message.writeUInt16LE(sender.port, msgIdx);

                    udpServer.send(message,
                                   0,
                                   message.length,
                                   destPort,
                                   destAddress,
                                   function (){
                                       console.log(currDate.toLocaleTimeString() +
                                                   ": znp msg from " +
                                                   sender.address + ":" + sender.port + ' cmd: ' +
                                                   parseInt(message.readUInt8(16)) + ' id: ' +
                                                   parseInt(message.readUInt8(17)));
                                   }
                    );

                    break;

                case OTA_MSG: // ---------------------------------------------------------------------------------------

                    var dataStartIdx;
                    var noImgMsg;

                    //var dataLen = message.readUInt8(msgIdx);
                    msgIdx++;
                    //var cmdType = message.readUInt8(msgIdx);
                    msgIdx++;
                    var cmdID = message.readUInt8(msgIdx);
                    msgIdx++;

                    var fileIdIdx = msgIdx; // --- file id start

                    var mfgID = message.readUInt16LE(msgIdx);
                    msgIdx += 2;
                    var imgType = message.readUInt16LE(msgIdx);
                    msgIdx += 2;
                    var fileVersion = message.readUInt32LE(msgIdx);
                    msgIdx += 4;

                    var fileIdLen = msgIdx - fileIdIdx;

                    // convert integers to hex zero-padded string
                    var mfgDir = ("0000" + mfgID.toString(16)).substr(-4).toUpperCase();
                    var imgTypeDir = ("0000" + imgType.toString(16)).substr(-4).toUpperCase();
                    var versionFile = ("00000000" + fileVersion.toString(16)).substr(-8).toUpperCase() + '.zigbee';

                    var addrIdx = msgIdx; // --- address start

                    var addrMode = message.readUInt8(msgIdx++);
                    if(addrMode == Addr16Bit){
                        msgIdx += 2; // short addr
                    }
                    else {
                        msgIdx += 8; // ext addr
                    }
                    msgIdx += 1; // end point
                    msgIdx += 2; // panID

                    var addrLen = msgIdx - addrIdx;

                    rspIdx = 0;
                    udpMsg.writeUInt32LE(PACKET_ID, rspIdx);
                    rspIdx += 4;
                    udpMsg.writeUInt32LE(OTA_MSG, rspIdx);
                    rspIdx += 4;

                    udpMsg.writeUInt32LE(0, rspIdx);
                    rspIdx += 4;
                    udpMsg.writeUInt16LE(0, rspIdx);
                    rspIdx += 2;
                    udpMsg.writeUInt8(msgSeqNo, rspIdx);
                    rspIdx++;

                    var lenIdx = rspIdx;

                    var dataRspLen = 0;
                    udpMsg.writeUInt8(dataRspLen, rspIdx); // data len will be updated later
                    rspIdx++;
                    udpMsg.writeUInt8(0, rspIdx); // cmd type, not used
                    rspIdx++;

                    switch(cmdID){

                        case MT_OTA_NEXT_IMG_REQ: // -------------------------------------------------------------------
                            // read rest of in msg
                            var nextImgOptions = message.readUInt8(msgIdx);
                            msgIdx++;
                            //var hwVersion = message.readUInt16LE(msgIdx);
                            msgIdx += 2;

                            udpMsg.writeUInt8(MT_OTA_NEXT_IMG_RSP, rspIdx); // cmd ID
                            rspIdx++;

                            dataStartIdx = rspIdx; // --- data start ---

                            udpMsg.writeUInt16LE(mfgID, rspIdx);
                            rspIdx += 2;
                            udpMsg.writeUInt16LE(imgType, rspIdx);
                            rspIdx += 2;
                            var fileVersionIdx = rspIdx;
                            udpMsg.writeUInt32LE(fileVersion, rspIdx); // will be eventually overwriten
                            rspIdx += 4;
                            // copy address from in msg
                            message.copy(udpMsg,
                                         rspIdx,
                                         addrIdx,
                                         addrLen);
                            rspIdx += addrLen;

                            var dirName = path.join(__dirname,
                                                    mfgDir,
                                                    imgTypeDir);
                            fs.readdir(dirName,
                                       function (err, files){
                                           if(err){
                                               // log error
                                               console.log(err);

                                               udpMsg.writeUInt8(ZOtaNoImageAvailable, rspIdx);
                                               rspIdx++;
                                               udpMsg.writeUInt8(nextImgOptions, rspIdx);
                                               rspIdx++;
                                               udpMsg.writeUInt32LE(0, rspIdx); // zero len
                                               rspIdx += 4;

                                               dataRspLen = rspIdx - dataStartIdx;
                                               udpMsg.writeUInt8(dataRspLen, lenIdx);

                                               noImgMsg = new Buffer(rspIdx);
                                               udpMsg.copy(noImgMsg,
                                                           0,
                                                           0,
                                                           rspIdx);
                                               udpServer.send(noImgMsg,
                                                              0,
                                                              noImgMsg.length,
                                                              sender.port,
                                                              sender.address,
                                                              function (){
                                                                  // ---
                                                              }
                                               );
                                           }
                                           else {
                                               var filesCnt = files.length;
                                               var newFileVersion = 0;
                                               var newFullFileName = '';
                                               if(filesCnt > 0){
                                                   for(var i = 0; i < filesCnt; i++){
                                                       var fileFullName = files[i];
                                                       var fileParts = fileFullName.split('.');
                                                       if(fileParts.length > 1){
                                                           var fileName = fileParts[0];
                                                           var currFileVersion = parseInt(fileName, 16);
                                                           var fileExt = fileParts[1].toLowerCase();
                                                           if(fileExt == 'zigbee'){
                                                               if(fileVersion != currFileVersion){
                                                                   newFileVersion = currFileVersion;
                                                                   newFullFileName = fileFullName;
                                                                   // overwrite file version
                                                                   udpMsg.writeUInt32LE(currFileVersion, fileVersionIdx);
                                                                   break; // pick up the fist valid
                                                               }
                                                           }
                                                       }
                                                   }
                                               }
                                               if(newFileVersion != 0){
                                                   fs.stat(path.join(dirName, newFullFileName),
                                                           function (err, stats){
                                                               if(err){
                                                                   // log error
                                                                   console.log(err);

                                                                   udpMsg.writeUInt8(ZOtaNoImageAvailable, rspIdx);
                                                                   rspIdx++;
                                                                   udpMsg.writeUInt8(nextImgOptions, rspIdx);
                                                                   rspIdx++;
                                                                   udpMsg.writeUInt32LE(0, rspIdx); // zero len
                                                                   rspIdx += 4;

                                                                   dataRspLen = rspIdx - dataStartIdx;
                                                                   udpMsg.writeUInt8(dataRspLen, lenIdx);

                                                                   noImgMsg = new Buffer(rspIdx);
                                                                   udpMsg.copy(noImgMsg,
                                                                               0,
                                                                               0,
                                                                               rspIdx);
                                                                   udpServer.send(noImgMsg,
                                                                                  0,
                                                                                  noImgMsg.length,
                                                                                  sender.port,
                                                                                  sender.address,
                                                                                  function (){
                                                                                      // ---
                                                                                  }
                                                                   );
                                                               }
                                                               else {
                                                                   udpMsg.writeUInt8(ZSuccess, rspIdx);
                                                                   rspIdx++;
                                                                   udpMsg.writeUInt8(nextImgOptions, rspIdx);
                                                                   rspIdx++;
                                                                   udpMsg.writeUInt32LE(stats['size'], rspIdx);
                                                                   rspIdx += 4;

                                                                   // write final data len
                                                                   dataRspLen = rspIdx - dataStartIdx;
                                                                   udpMsg.writeUInt8(dataRspLen, lenIdx);

                                                                   var newImgMsg = new Buffer(rspIdx);
                                                                   udpMsg.copy(newImgMsg,
                                                                               0,
                                                                               0,
                                                                               rspIdx);
                                                                   udpServer.send(newImgMsg,
                                                                                  0,
                                                                                  newImgMsg.length,
                                                                                  sender.port,
                                                                                  sender.address,
                                                                                  function (){
                                                                                      // ---
                                                                                  }
                                                                   );
                                                               }
                                                   });
                                               }
                                               else {
                                                   udpMsg.writeUInt8(ZOtaNoImageAvailable, rspIdx);
                                                   rspIdx++;
                                                   udpMsg.writeUInt8(nextImgOptions, rspIdx);
                                                   rspIdx++;
                                                   udpMsg.writeUInt32LE(0, rspIdx); // zero len
                                                   rspIdx += 4;

                                                   dataRspLen = rspIdx - dataStartIdx;
                                                   udpMsg.writeUInt8(dataRspLen, lenIdx);

                                                   var noNewImgMsg = new Buffer(rspIdx);
                                                   udpMsg.copy(noNewImgMsg,
                                                               0,
                                                               0,
                                                               rspIdx);
                                                   udpServer.send(noNewImgMsg,
                                                                  0,
                                                                  noNewImgMsg.length,
                                                                  sender.port,
                                                                  sender.address,
                                                                  function (){
                                                                      // ---
                                                                  }
                                                   );
                                               }
                                           }
                            });

                            break;

                        case MT_OTA_FILE_READ_REQ: // ------------------------------------------------------------------
                            // read rest of in msg
                            var fileOffset = message.readUInt32LE(msgIdx);
                            msgIdx += 4;
                            var maxDataSize = message.readUInt8(msgIdx++);

                            udpMsg.writeUInt8(MT_OTA_FILE_READ_RSP, rspIdx);
                            rspIdx++;

                            dataStartIdx = rspIdx; // --- data start ---

                            message.copy(udpMsg,
                                         rspIdx,
                                         fileIdIdx,
                                         fileIdLen);
                            rspIdx += fileIdLen;
                            message.copy(udpMsg,
                                         rspIdx,
                                         addrIdx,
                                         addrLen);
                            rspIdx += addrLen;

                            var filePath = path.join(__dirname,
                                                     mfgDir,
                                                     imgTypeDir,
                                                     versionFile);
                            fs.open(filePath,
                                    'r',
                                    function(err, fd){
                                        var abortMsg;
                                        if(err){
                                            // log error
                                            console.log(err);

                                            udpMsg.writeUInt8(ZCL_STATUS_ABORT, rspIdx);
                                            rspIdx++;

                                            dataRspLen = rspIdx - dataStartIdx;
                                            udpMsg.writeUInt8(dataRspLen, lenIdx);

                                            abortMsg = new Buffer(rspIdx);
                                            udpMsg.copy(abortMsg,
                                                        0,
                                                        0,
                                                        rspIdx);
                                            udpServer.send(abortMsg,
                                                           0,
                                                           abortMsg.length,
                                                           sender.port,
                                                           sender.address,
                                                           function (){
                                                               // ---
                                                           }
                                            );
                                            if(fd){
                                                fs.close(fd);
                                            }
                                        }
                                        else {
                                            var dataBuf = new Buffer(maxDataSize);
                                            fs.read(fd,
                                                    dataBuf,
                                                    0,
                                                    maxDataSize,
                                                    fileOffset,
                                                    function (err, bytesRead, buffer){
                                                        if(err){
                                                            console.log(err);

                                                            udpMsg.writeUInt8(ZCL_STATUS_ABORT, rspIdx);
                                                            rspIdx++;

                                                            dataRspLen = rspIdx - dataStartIdx;
                                                            udpMsg.writeUInt8(dataRspLen, lenIdx);

                                                            abortMsg = new Buffer(rspIdx);
                                                            udpMsg.copy(abortMsg,
                                                                        0,
                                                                        0,
                                                                        rspIdx);

                                                            udpServer.send(abortMsg,
                                                                           0,
                                                                           abortMsg.length,
                                                                           sender.port,
                                                                           sender.address,
                                                                           function (){
                                                                               // ---
                                                                           }
                                                            );
                                                        }
                                                        else {
                                                            udpMsg.writeUInt8(ZCL_STATUS_SUCCESS, rspIdx);
                                                            rspIdx++;
                                                            udpMsg.writeUInt32LE(fileOffset, rspIdx);
                                                            rspIdx += 4;
                                                            udpMsg.writeUInt8(bytesRead, rspIdx);
                                                            rspIdx++;
                                                            buffer.copy(udpMsg,
                                                                        rspIdx,
                                                                        0,
                                                                        bytesRead);
                                                            rspIdx += bytesRead;

                                                            dataRspLen = rspIdx - dataStartIdx;
                                                            udpMsg.writeUInt8(dataRspLen, lenIdx);

                                                            var fileReadMsg = new Buffer(rspIdx);
                                                            udpMsg.copy(fileReadMsg,
                                                                        0,
                                                                        0,
                                                                        rspIdx);
                                                            udpServer.send(fileReadMsg,
                                                                           0,
                                                                           fileReadMsg.length,
                                                                           sender.port,
                                                                           sender.address,
                                                                           function (){
                                                                               // ---
                                                                           }
                                                            );
                                                        }
                                                        if(fd){
                                                            fs.close(fd);
                                                        }
                                            });
                                        }
                            });
                            break;
                    }
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

    mcastServer.on(
        "listening",
        function () {
            var mcastServerAddress = mcastServer.address();
            console.log("UDP Server listening at: " +
                        mcastServerAddress.address +
                        ":" +
                        mcastServerAddress.port);
            mcastServer.setBroadcast(true);
            mcastServer.setMulticastTTL(128);
            mcastServer.addMembership(MCAST_SERVER_IP);
        }
    );

    mcastServer.on(
        "message",
        function (mcastMsg, mcastSender) {
            var mcastMsgIdx = 0;
            var mcastPktID = mcastMsg.readUInt32LE(mcastMsgIdx);
            mcastMsgIdx += 4;
            if(mcastPktID != PACKET_ID){
                console.log("bad Packet ID " +
                            mcastSender.address +
                            ":" +
                            mcastSender.port);
                return;
            }
            var mcastPktFunc = mcastMsg.readUInt32LE(mcastMsgIdx);
            mcastMsgIdx += 4;

            mcastMsgIdx += 4; // skip src address
            mcastMsgIdx += 2; // skip src port

            var msgSeqNo = mcastMsg.readUInt8(mcastMsgIdx);
            mcastMsgIdx++;

            switch (mcastPktFunc){
                case MCAST_TEST:
                    console.log("multiCast Test " +
                                mcastSender.address +
                                ":" +
                                mcastSender.port);
                    break;
            }
        }
    );

    mcastServer.bind(MCAST_SERVER_PORT);
