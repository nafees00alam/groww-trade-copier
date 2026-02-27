# Deployment

## systemd Service

Create `/etc/systemd/system/groww-copier.service`:

```ini
[Unit]
Description=Groww F&O Trade Copier
After=network.target

[Service]
Type=simple
User=sdms
WorkingDirectory=/home/sdms/groww-copier
ExecStart=/home/sdms/groww-copier/.venv/bin/python app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable groww-copier
sudo systemctl start groww-copier
```

## Checking Logs

```bash
journalctl -u groww-copier -f              # live tail
journalctl -u groww-copier --since "1h ago" # last hour
```

## Deploying Updates

```bash
# From local machine
scp app.py root@your-server:/home/sdms/groww-copier/
ssh root@your-server "chown sdms:sdms /home/sdms/groww-copier/app.py"

# Restart
ssh root@your-server "PID=\$(systemctl show groww-copier -p MainPID --value) && kill -9 \$PID; sleep 2; systemctl start groww-copier"
```

## Port

The app runs on port **8002** by default. Access the dashboard at `http://your-server:8002`.

## Environment

- Python 3.10+ with venv
- `config.json` must be in the working directory
- `copied_orders.json` is auto-created for order state persistence
