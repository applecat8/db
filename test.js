const {spawn} = require('child_process');
const child = spawn('./a.out', ['mydata.db']);
child.stdout.pipe(process.stdout);

function run_script(commands) {
    commands.forEach(command => {
        child.stdin.write(`${command}\n`);
    });
}

const commands = [];
for (let i = 1; i < 1401; i++) {
    commands.push(`insert ${i} user${i} person${i}@example.com`);
}
commands.push('.exit');

run_script(commands);
