const {spawn} = require('child_process');
const child = spawn('./a.out');
process.stdin.pipe(child.stdin);
child.stdout.on('data', (data) => {
    console.log(`child stdout: ${data}`);
});

child.stdin.write('select')
child.()
