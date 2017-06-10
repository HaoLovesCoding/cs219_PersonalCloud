// This is a backgroup JS that requries node.js to execute
// This will only detect USB portable devices
// This works on all platform that has node js and python > 2.4 (2.x)

var usbDetect = require('usb-detection');

usbDetect.on('add', function(device) {
	console.log(device)
    console.log('add,' + device.vendorId + ',' + device.productId + ',' + device.manufacturer + ',' + device.deviceName);
});

usbDetect.on('remove', function(device) {
    console.log('remove,' + device.vendorId + ',' + device.productId + ',' + device.manufacturer + ',' + device.deviceName);
});

var readline = require('readline');
var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

rl.on('line', function(line){
    if (line == 'stop'){
    	usbDetect.stopMonitoring();
    	rl.close()
    }
});