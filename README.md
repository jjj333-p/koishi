# Koishi Bridge

The fair and benevolent bridge project. Her jealousy was Parsee's weakness, but Koishi is the fair and benevolent princess here to make everything right. 

In all seriousness, for now, this bridge is a Matrix <-> XMPP bridge, picking up the [Parsee Bridge](https://forge.fsky.io/lda/Parsee) left off, more ambitious in terms of features, 
less [NIH syndrome](https://www.joelonsoftware.com/2001/10/14/in-defense-of-not-invented-here-syndrome/). I hope to eventually add support for IRC, and perhaps even pooprietary platforms eventually.

### This project is in very early development, you will need to change bits of code to get it to work for you!!

This project is 100% Python, I'm not a huge fan of it, but for the most part its the greatest common denominator between these platforms. Also I'd rather a bridge that exists now, someone else can make a more performant one later.

Codebase note: I've tried to separate each general area of the program to a different file, I.e. Matrix gets one file, XMPP gets one file, webserver gets a file, DB gets a file, etc. For the most part anything that is a prerequisite
to a section of code will be above the code (in terms of like functions and whatnot)

Licensed under AGPLv3

----

## Currently Supported Features:

- XMPP Puppeting (slixmpp component)
- Bridging of messages
    - persistent id storage for reply bridging
    - media
        - fake Auth Media + redirect to original url for XMPP -> Matrix bridging
        - download and re-serve matrix media to a url for XMPP
    - read receipt on matrix side to confirm delivery to XMPP
<img width="1037" height="549" alt="image" src="https://github.com/user-attachments/assets/c2ec3da6-48bf-46aa-b3b1-fa33e94ff272" />
<img width="1091" height="521" alt="image" src="https://github.com/user-attachments/assets/ebf82ec6-6644-457f-9148-c2cac930501e" />

 
## TODO:
Ranked by priority, marked by percieved difficulty if you wanted to PR

- Bridge message deletes (low-medium difficulty)
    - Incl. Deleting media record (+ difficulty)
- Bridge Bans (Difficult)
- Bridge Read Receipts (easy)
- Bridge Reactions (medium, requires storing in db due to different formats)
    - Matrix is 1 reaction per event while XMPP is last reaction event contains the list of your current reactions
- Configurability of rooms (low-high depending)
    - moving room bridging config to config file may be a decent stopgap
    - Preferred to be able to configure it through the bot for public instances (perhaps not worth it)
- Add puppeting on Matrix side (medium - high)
- bridge pfps (annoying, but prob not hard)
- Add IRC support (???)

--- 
