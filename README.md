# Koishi Bridge

The fair and benevolent bridge project. Her jealousy was Parsee's weakness, but Koishi is the fair and benevolent princess here to make everything right. 

In all seriousness, Koishi is a Matrix room <-> XMPP MUC bridge, picking up where [Parsee Bridge](https://foundry.fsky.io/kappachat/Parsee) left off, more ambitious in terms of features, 
less [NIH syndrome](https://www.joelonsoftware.com/2001/10/14/in-defense-of-not-invented-here-syndrome/). 

### Support Room:

**XMPP:** [koishi@group.pain.agency](xmpp:koishi@group.pain.agency?join)

**Matrix:** [#koishi:4d2.org](https://matrix.to/#/#koishi:4d2.org)

### Tip me:

[pain.agency/tip](https://pain.agency/tip)

### This project is in very early development, you likely can get it working, however expect things to be broken or missing, and rough edges!!

This project is 100% Python, I'm not a huge fan of it, but for the most part its the greatest common denominator between these platforms. Also I'd rather a bridge that exists now, someone else can make a more performant one later.

Codebase note: I've tried to separate each general area of the program to a different file, I.e. Matrix gets one file, XMPP gets one file, webserver gets a file, DB gets a file, etc. For the most part anything that is a prerequisite
to a section of code will be above the code (in terms of like functions and whatnot)

Licensed under AGPLv3

----
## Configuration

Create a file login.json that looks like this. 
```json
{
    "vanity_name": "Koishi Bridge",
    "matrix": {
        "domain": "https://matrix.4d2.org",
        "mxid": "@koishi-jjj333_p:4d2.org",
        "password": "[redacted]"
    },
    "xmpp": {
        "jid": "koishi.pain.agency",
        "secret": "[redacted]",
        "domain": "localhost",
        "port": 5347
    },
    "bridge-mapping": [
        {
            "xmpp": "chaos@group.pain.agency",
            "matrix": "!odwJFwanVTgIblSUtg:matrix.org"
        },
        ...
    ],
    "http_domain": "koishi.pain.agency",
    "postgresql_conn": "postgresql://koishi:[password]@localhost:[port]/koishi",
}
```

----
## Running with Docker (optional)

This is an alternative to installing Python and the dependencies on your machine yourself. Docker packages Koishi and everything it needs (Python 3.13.5 and the pip packages) into a self-contained "image". You only need [Docker](https://docs.docker.com/get-docker/) installed.

Docker support is community-contributed, so if something breaks, please open an issue.

### 1. Create your config

Koishi reads its settings from a file called `login.json` in the project folder (see the Configuration section above for what goes in it). Create it before you continue.

`login.json` holds passwords, so it is deliberately **not** copied into the Docker image (see `.dockerignore`). You give it to the container when you start it, in step 3.

### 2. Build the image

From the project folder, run:

```
docker build -t koishi .
```

This reads the `Dockerfile` and builds an image named `koishi`. You only need to repeat it when the code changes.

### 3. Run it

```
docker run --rm -p 4567:4567 -v "$(pwd)/login.json:/app/login.json:ro" koishi
```

What this does:
- `--rm` deletes the container when it stops, so stopped containers don't pile up.
- `-p 4567:4567` makes the media webserver (which listens on port 4567 inside the container) reachable on port 4567 of your machine.
- `-v "$(pwd)/login.json:/app/login.json:ro"` shares your `login.json` with the container as a read-only file. The file stays on your machine and never becomes part of the image.
- `koishi` is the image you built in step 2.

Press `Ctrl+C` to stop it. To keep it running in the background, add `-d` and a name: `docker run -d --name koishi-bridge -p 4567:4567 -v "$(pwd)/login.json:/app/login.json:ro" koishi`. Then use `docker logs koishi-bridge` to see its output.

### Things to know

- **Koishi still needs a PostgreSQL server and an XMPP server it can reach.** Docker only runs Koishi itself; it does not start these for you.
- **Do not use `localhost` for those servers in `login.json`.** Inside a container, `localhost` means the container itself, not your computer, so Koishi can't find your database that way. Use the server's real hostname or IP address instead.
- **"No such file or directory: 'login.json'"** means the `-v` part of the run command is missing, or the command was run from a folder that doesn't contain `login.json`.
- **"Connection refused"** means Koishi started fine but can't reach the database or XMPP server at the address in `login.json`.
- The image uses Python 3.13.5, matching the version the maintainer runs in production.

----

## Currently Supported Features:

- XMPP Puppeting (slixmpp component)
- Matrix "puppeting" via proposed [msc4144](https://github.com/beeper/matrix-spec-proposals/blob/per-message-profile/proposals/4144-per-message-profile.md)
- Bridging of messages
    - formatting conversion between XEP-0393: Message Styling and Matrix HTML styling (TODO: update screenshots?)
    - persistent id storage for reply bridging
    - media
        - fake Auth Media + redirect to original url for XMPP -> Matrix bridging
        - download and re-serve Matrix media to a url for XMPP
    - read receipt on Matrix side to confirm delivery to XMPP
    - read receipt bridging from Matrix to XMPP
- Corrections/edits
    - XMPP -> Matrix replies resolve if its a correction and reply to the original event
- automatic voicing of puppets
- bridging of moderations (XMPP), Retractions (XMPP), and Redactions (Matrix)
    - TODO: ensure server support
- bridging of kicks and bans of XMPP puppets to the Matrix side
- configuration of bridging multiple rooms (TODO: document)
    - Automatic reconnection to MUCs on disconnect
<img width="1037" height="549" alt="image" src="https://github.com/user-attachments/assets/c2ec3da6-48bf-46aa-b3b1-fa33e94ff272" />
<img width="1091" height="521" alt="image" src="https://github.com/user-attachments/assets/ebf82ec6-6644-457f-9148-c2cac930501e" />
<img width="1302" height="926" alt="image" src="https://github.com/user-attachments/assets/691c4b52-1af1-44a2-8b6d-fb115e088404" />
<img width="1043" height="97" alt="image" src="https://github.com/user-attachments/assets/e223dd39-11f7-40d3-8bee-f389151834aa" />
<img width="1056" height="89" alt="image" src="https://github.com/user-attachments/assets/f03dc8de-e8b3-4cb7-8247-87fff55c2b6c" />

## TODO:
Ranked by priority, marked by percieved difficulty if you wanted to PR

- automatically disambiguate puppets if theres duplicate nicks on Matrix side (low-med)
- bridge leaves from Matrix to XMPP (lowish)
- Bridge Bans (medium)
- Bridge Reactions (difficult, requires storing in db due to different formats)
    - Matrix is 1 reaction per event while XMPP is last reaction event contains the list of your current reactions
- When bridging message deletions, ensure server support
- Add puppeting on Matrix side (VERY HIGH)
    - bridge read receipts XMPP -> Matrix
    - use puppet to let Matrix auth verify retractions
    - maunium-appservice is undocumented and basically abandonware, we'll have to implement the appservice api manually
- bridge pfps
    - Matrix -> XMPP hard as slixmpp only has nice handling for clients not servers/components
        - maybe base on [Jabbagram](https://github.com/ventureoo/jabagram/commit/5829a5525eea891d03a6b16843a387b055eaf44f)
    - XMPP -> Matrix mainly tedious and long
- Configurability of rooms through the bot for public instances (high, perhaps not worth it)

---- 

## Contributors 

- [jjj333_p](https://pain.agency) (obviously)
- [qwertydotplus](https://qwertydotplus.cc/)
