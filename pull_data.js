const { spawn } = require('child_process');

exports.pull_data = () => {
    // Define source and destination
    const source = '\\\\awlnutdata\\MnR_Sales';
    const destination = 'D:\\Projects\\25NodeVersion\\xlsx_data';

    // Use spawn to execute robocopy command
    const robocopy = spawn('robocopy', [source, destination]);

    // Handle stdout (output)
    robocopy.stdout.on('data', (data) => {
        console.log(`Stdout: ${data}`);
    });

    // Handle stderr (error output)
    robocopy.stderr.on('data', (data) => {
        console.error(`Stderr: ${data}`);
    });

    // Handle process exit
    robocopy.on('close', (code) => {
        console.log(`Process exited with code ${code}`);
    });

}