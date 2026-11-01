"""One short lesson per feature, so someone new can learn from the page alone.

Each lesson answers four questions in plain words:
  problem    why does this feature exist?
  use_when   when would I use it?
  steps      how does it work?
  run_shows  what will I see when I press Run?
"""

GROUP_INTROS = {
    "Topics and messages": "Start here. How messages are named, stored and ordered.",
    "Producers": "How a program sends messages quickly and safely.",
    "Consumers": "How programs read messages, share work and recover from problems.",
    "Transactions and exactly-once": "How to make sure work happens one time only, even after a crash.",
    "Storage and retention": "Where messages live on disk, and when they are removed.",
    "The cluster": "How several computers work as one, and survive a failure.",
    "Stream processing": "How to calculate useful results from messages as they arrive.",
    "Security": "How to control who can connect, and what they can do.",
    "Ecosystem": "Popular tools that work with Kafka. These need the full version to run.",
}

LESSONS = {
    "topic-admin": {
        "problem": "Every kind of message needs a place to live, with the right size and rules. Doing this by hand on many systems is slow and causes mistakes.",
        "use_when": "When you set up a new kind of data, or need more capacity for an existing one.",
        "steps": [
            "A program asks Kafka to create a topic with a name, a number of parts and a number of copies.",
            "Kafka spreads the parts and copies across the brokers.",
            "Later the program can add parts or change a rule, such as how long to keep data.",
            "When the topic is no longer needed, the program deletes it.",
        ],
        "run_shows": "Three PASS lines: the parts grew from 3 to 6, the keep time changed to 2 hours, and the topic was deleted.",
    },
    "offsets": {
        "problem": "With millions of messages, you need a simple way to find one exact message and to know what comes next.",
        "use_when": "Always. Every message in Kafka has an offset, and every reader uses them.",
        "steps": [
            "A topic is split into parts called partitions.",
            "Each new message is added to the end of one part.",
            "That part gives it the next number: 0, 1, 2, and so on.",
            "To find a message you need three things: the topic, the part and the number.",
        ],
        "run_shows": "A table of where 60 messages landed. Each part has its own numbers, and each starts from 0.",
    },
    "keys": {
        "problem": "Parts work at the same time, so messages in different parts have no fixed order. A trip could look completed before it was accepted.",
        "use_when": "When the order of related messages matters, such as all events of one driver or one customer.",
        "steps": [
            "You give each message a key, such as the driver's ID.",
            "Kafka turns the key into a large number, using a hash.",
            "It divides that number by the number of parts. The remainder picks the part.",
            "The same key always gives the same remainder, so it always goes to the same part.",
        ],
        "run_shows": "A table with each driver's real hash and the part it chose, then proof that all 20 events of each driver came back in order.",
    },
    "no-key": {
        "problem": "If order does not matter, there is no need to tie messages to one part. Spreading them keeps all the parts equally busy.",
        "use_when": "For data with no relation between messages, such as page view counts or sensor readings.",
        "steps": [
            "You send a message without a key.",
            "The producer picks a part for it.",
            "Over many messages, all the parts receive a share.",
        ],
        "run_shows": "A table of how 300 messages spread across all 6 parts.",
    },
    "custom-partitioner": {
        "problem": "The normal hash treats every key the same. Sometimes one kind of message needs its own lane, so it can be handled first.",
        "use_when": "When a business rule should decide the part, such as VIP customers or a region.",
        "steps": [
            "You write a small function that receives the key.",
            "The function returns the part to use.",
            "The producer calls your function for every message.",
        ],
        "run_shows": "A table showing that VIP keys all used part 0, and normal keys never did.",
    },
    "headers": {
        "problem": "Sometimes a program needs a little information, such as where a message came from, without opening and parsing the whole message.",
        "use_when": "To carry a tracking id, a format version or the sender's name beside the message.",
        "steps": [
            "The producer adds name and value pairs to the message.",
            "Kafka stores them next to the message.",
            "The reader can check them before it reads the message itself.",
        ],
        "run_shows": "A table of three headers read back from Kafka, exactly as they were sent.",
    },
    "timestamps": {
        "problem": "The time on a message can matter a lot, but the clocks on senders can be wrong.",
        "use_when": "Use broker time when you cannot trust the sender's clock. Use sender time when the real moment of the event matters.",
        "steps": [
            "By default, the producer's time is stored on the message.",
            "A topic can be set to LogAppendTime instead.",
            "Then the broker replaces the time with its own clock when the message arrives.",
        ],
        "run_shows": "Two topics receive a message claiming to be one hour old. One keeps that time, and the other replaces it with the time now.",
    },
    "message-size": {
        "problem": "Very large messages slow down everyone and use a lot of memory, so Kafka needs a limit.",
        "use_when": "Keep the default limit unless you have a good reason. For big files, store the file elsewhere and send a link.",
        "steps": [
            "Each topic has a maximum message size.",
            "The broker checks every message when it arrives.",
            "A message over the limit is refused, and the producer gets an error.",
        ],
        "run_shows": "On a topic limited to 2 KB, a 1 KB message is accepted and a 5 KB message is refused.",
    },

    "acks": {
        "problem": "After sending a message, how do you know it is safe? Waiting longer is safer, but slower.",
        "use_when": "Use all for money and orders. Use 1 for normal data. Use 0 only when losing a few messages does not matter.",
        "steps": [
            "acks=0: send it and move on, without waiting for any reply.",
            "acks=1: wait until the leader broker has saved it.",
            "acks=all: wait until every in-sync copy has saved it.",
        ],
        "run_shows": "A table comparing the time for 300 messages at each level. With acks=0 no offset comes back, because the broker never replied.",
    },
    "min-insync": {
        "problem": "acks=all means all healthy copies. If only one copy is healthy, all means one, and your data is less safe than you think.",
        "use_when": "On important topics. A common setup is 3 copies with a minimum of 2.",
        "steps": [
            "You set the minimum number of healthy copies a topic needs.",
            "Before saving an acks=all message, the broker counts the healthy copies.",
            "If there are too few, it refuses the write and returns an error.",
        ],
        "run_shows": "With all brokers running, the write succeeds. Stop a broker and press Run again: this time the write is refused.",
    },
    "batching": {
        "problem": "Sending each message on its own wastes time, because every send needs a trip across the network.",
        "use_when": "Almost always. Batching is why Kafka can handle so many messages.",
        "steps": [
            "The producer holds messages for a very short time, such as 10 milliseconds.",
            "Messages going to the same part are packed together.",
            "The whole pack goes out in one network trip.",
        ],
        "run_shows": "500 messages sent both ways, with the time taken and the real number of batches found in Kafka's files on disk.",
    },
    "compression": {
        "problem": "Text data such as JSON repeats itself, so storing and sending it as it is wastes disk space and network.",
        "use_when": "Almost always for text data. Each method trades speed against how small the data gets.",
        "steps": [
            "The producer compresses a whole batch at once.",
            "The broker stores it compressed.",
            "The consumer unpacks it automatically, so your code does not change.",
        ],
        "run_shows": "A table of the real size on disk of 3,000 ride events with each method, and how many times smaller each one is.",
    },
    "idempotence": {
        "problem": "Networks fail. If a reply is lost, the sender cannot know whether the message was saved, so it sends it again and creates a duplicate.",
        "use_when": "Almost always. It is a safe default for producers.",
        "steps": [
            "Each producer gets an id, and numbers every message it sends.",
            "The broker remembers the last number it saved.",
            "If the same number arrives again, the broker drops the copy and sends the reply again.",
        ],
        "run_shows": "A reply is really lost in both runs. With the feature off, the message is stored twice. With it on, it is stored once.",
    },
    "quota": {
        "problem": "One program sending too much can slow down every other program using the same cluster.",
        "use_when": "When many teams or programs share one cluster.",
        "steps": [
            "You set a speed limit for a client name.",
            "The broker measures that client's speed over several seconds.",
            "If it goes over the limit, the broker delays its replies, which slows it down.",
        ],
        "run_shows": "The same data sent by a free client and a limited client. The limited one is many times slower, and nothing is lost. This takes about 15 seconds.",
    },

    "group-share": {
        "problem": "One reader may be too slow for all the messages. You need several readers without reading the same message twice.",
        "use_when": "When a single copy of your program cannot keep up.",
        "steps": [
            "You start several copies of your program with the same group id.",
            "Kafka gives each copy different parts.",
            "Each part has one owner, so no message is handled twice by the group.",
        ],
        "run_shows": "A table of which reader owns which parts, then proof that together they read all 60 messages exactly once.",
    },
    "rebalance": {
        "problem": "Programs crash and restart. If a reader disappears, somebody must take over its work, without a person doing it.",
        "use_when": "It happens by itself whenever readers join or leave a group.",
        "steps": [
            "Each reader regularly tells Kafka that it is alive.",
            "When a reader leaves or goes silent, Kafka notices.",
            "Kafka gives that reader's parts to the others, who continue from the last bookmark.",
        ],
        "run_shows": "A before and after table of who owns what, and how fast the handover was. No message is lost.",
    },
    "pub-sub": {
        "problem": "Several systems often need the same data. Without this, the sender would have to send it to each one separately.",
        "use_when": "Whenever two different jobs need the same messages, such as billing and analytics in RideFlow.",
        "steps": [
            "Each job uses its own group id.",
            "Kafka keeps a separate bookmark for each group.",
            "Each group reads every message at its own speed.",
        ],
        "run_shows": "Two groups read the same topic, and each receives all 30 messages.",
    },
    "assignors": {
        "problem": "The way parts are divided among readers affects how even the work is, and how much moves during a change.",
        "use_when": "When one reader in a group gets much more work than the others.",
        "steps": [
            "Range divides each topic separately, which can be uneven.",
            "Round robin deals the parts out one at a time, like playing cards.",
            "Sticky is even too, and tries not to move parts when the group changes.",
        ],
        "run_shows": "A table comparing the three rules on the same topics. Range gives 4 and 2, and the others give 3 and 3.",
    },
    "static-membership": {
        "problem": "Every time a reader restarts, the whole team normally stops and reorganises. In a planned restart, that is a waste.",
        "use_when": "For services that restart often, such as during updates.",
        "steps": [
            "You give the reader a fixed instance name.",
            "When it stops, Kafka keeps its place for a short time.",
            "When it returns with the same name, it gets the same work back, with no reorganising.",
        ],
        "run_shows": "A table of how many times the team reorganised after one restart: at least 1 for a normal reader, and 0 for a fixed one.",
    },
    "offset-reset": {
        "problem": "A brand new reader has no bookmark, so Kafka needs to know where it should begin.",
        "use_when": "Use earliest to process all history. Use latest when only new data matters.",
        "steps": [
            "A new group joins with no saved bookmark.",
            "Kafka checks the auto_offset_reset setting.",
            "earliest starts at the oldest message, latest waits for new ones, and none raises an error.",
        ],
        "run_shows": "A table of all three settings on the same topic: 5 old messages read, only the 1 new message read, and an error.",
    },
    "delivery": {
        "problem": "A crash can happen between doing the work and saving your place. You must decide which risk is worse: losing a message or repeating it.",
        "use_when": "You make this choice in every consumer. Most systems choose to repeat, because a repeat can be detected.",
        "steps": [
            "At most once: save the bookmark first, then work. A crash loses the unfinished messages.",
            "At least once: work first, then save the bookmark. A crash repeats the finished messages.",
        ],
        "run_shows": "Both orders crash after finishing 6 of 10 messages. A table shows which messages were lost in one, and repeated in the other.",
    },
    "seek": {
        "problem": "Sometimes you need one exact message, or everything from a certain time, without reading from the beginning.",
        "use_when": "To look at one message, or to reprocess the data from one bad hour.",
        "steps": [
            "A reader can jump to a line number with seek.",
            "It can jump to the very beginning or the very end.",
            "It can ask Kafka for the first message after a certain time.",
        ],
        "run_shows": "Four PASS lines: a jump to offset 7, to the beginning, to the end, and a search by time that finds the right message.",
    },
    "reset-offsets": {
        "problem": "If a bug handled messages badly, you need to process them again. They are still in Kafka, so only the bookmark has to move.",
        "use_when": "After fixing a bug, or when you want to rebuild results from old data.",
        "steps": [
            "Stop the team's normal reading.",
            "Save a new, earlier bookmark for the group.",
            "When the group starts again, it reads from that earlier point.",
        ],
        "run_shows": "A group finishes 10 messages, then its bookmark moves back to 3, and it reads offsets 3 to 9 again.",
    },
    "pause-resume": {
        "problem": "If the system behind one part is busy, you do not want to stop everything, or leave the group.",
        "use_when": "When one part of your work must wait, but the rest can continue.",
        "steps": [
            "The reader tells its client to pause one part.",
            "It keeps reading the other parts, and stays in the group.",
            "When ready, it resumes, and that part continues from where it stopped.",
        ],
        "run_shows": "While part 0 is paused, only part 1 is read. After resuming, part 0 delivers its 5 messages.",
    },
    "lag": {
        "problem": "A system can look healthy while its reader falls further and further behind. You need a number that shows this.",
        "use_when": "Always monitor it in a real system. It is the most important number to watch.",
        "steps": [
            "Kafka knows the last line number written.",
            "It also knows the group's saved bookmark.",
            "The difference between them is the lag.",
        ],
        "run_shows": "A table with 100 messages written and 30 finished, so the lag is 70.",
    },
    "dead-letter": {
        "problem": "Because messages are read in order, one broken message can crash a reader again and again, and block everything behind it.",
        "use_when": "In any consumer that handles data it cannot fully trust.",
        "steps": [
            "The reader tries to process a message.",
            "If it fails, the message goes to a separate topic, with the reason.",
            "The reader moves on. Later, someone fixes the problem and sends the message again.",
        ],
        "run_shows": "Of 10 messages, 9 are processed and 1 broken message is parked. After a fix, that one is sent again and works.",
    },
    "retry-topic": {
        "problem": "Some failures are temporary, such as a database restart. Waiting inside the main stream would stop all the other messages.",
        "use_when": "When a failure is likely to fix itself in a short time.",
        "steps": [
            "The failed message goes to a retry topic, with a time to try again.",
            "The main stream keeps moving.",
            "When the time comes, a reader tries the message again.",
        ],
        "run_shows": "Messages 0 and 2 finish first. Message 1 waits for its delay, then succeeds.",
    },

    "txn-commit-abort": {
        "problem": "Some writes only make sense together. If only half of them are saved, your data is wrong.",
        "use_when": "When several messages, or several topics, must be updated together.",
        "steps": [
            "The producer begins a transaction.",
            "It writes one or more messages.",
            "It then commits to keep them, or aborts to cancel them.",
        ],
        "run_shows": "One message is committed and one aborted. A careful reader sees only the committed one, and a normal reader sees both.",
    },
    "isolation": {
        "problem": "A transaction might still be cancelled, so a reader who acts on it too early could act on data that never really happened.",
        "use_when": "Set read_committed on every reader of a transactional topic.",
        "steps": [
            "A producer opens a transaction and writes a message.",
            "A careful reader stops at the last finished point.",
            "When the transaction commits, the reader receives the message.",
        ],
        "run_shows": "While the transaction is open, the normal reader sees the message and the careful reader does not. After commit, both do.",
    },
    "exactly-once": {
        "problem": "If a program saves its result, then crashes before saving its bookmark, it will do the same work again and create duplicates.",
        "use_when": "For money, billing or anything that must never be counted twice.",
        "steps": [
            "Read messages from the input topic.",
            "Inside one transaction, write the results and save the bookmark.",
            "If anything fails, both are cancelled, and the work is simply done again.",
        ],
        "run_shows": "The first attempt crashes before commit, and the second succeeds. The log holds 40 messages, but readers see exactly 20.",
    },
    "pipeline-audit": {
        "problem": "A claim that a system never loses or repeats data should be proved, not trusted.",
        "use_when": "After a crash or failure, to prove that billing is still correct.",
        "steps": [
            "Read every ride event from Kafka.",
            "Work out which invoices should exist.",
            "Read every invoice, and compare the two lists one by one.",
        ],
        "run_shows": "A table of completed trips, invoices, missing invoices and duplicates. Missing and duplicates must both be 0.",
    },
    "fencing": {
        "problem": "A frozen program can wake up after its replacement has started. Then two copies could both write, and damage the data.",
        "use_when": "It protects every transactional producer automatically.",
        "steps": [
            "Each transactional producer has a fixed id.",
            "When a new copy starts with that id, Kafka gives it a higher generation number.",
            "Any write from the older copy is refused.",
        ],
        "run_shows": "An old producer stalls, a new one starts, and the old one is rejected. Only the new one's message is visible.",
    },

    "segments": {
        "problem": "One giant file would be very hard to clean up. Removing old data from the start of it would be slow.",
        "use_when": "It always happens. Knowing about it helps you understand how Kafka uses disk space.",
        "steps": [
            "Each part is a folder on disk.",
            "New messages are added to the newest file.",
            "When that file gets big or old, Kafka closes it and starts a new one.",
        ],
        "run_shows": "A list of real files read from broker 1's disk, each named after the first offset it holds.",
    },
    "retention": {
        "problem": "Disks are not endless. Old data must leave at some point, even though reading does not remove it.",
        "use_when": "Set it for every topic, based on how long the data is useful.",
        "steps": [
            "You set how long a topic keeps data.",
            "The broker regularly looks for closed files that are too old.",
            "It deletes those files, and the first available offset moves forward.",
        ],
        "run_shows": "A topic that keeps data for 1 second. The first offset moves from 0 to 10 or more, so the old messages are gone.",
    },
    "delete-records": {
        "problem": "Sometimes bad data must be removed now, without waiting days for retention.",
        "use_when": "To remove bad data, or to clear a topic used as a temporary store.",
        "steps": [
            "You choose a line number.",
            "You ask Kafka to delete everything before it.",
            "The first available offset moves up to that number.",
        ],
        "run_shows": "Of 10 messages, the first 5 are deleted. Offsets 5 to 9 remain.",
    },
    "compaction": {
        "problem": "For data like a driver's current status, only the newest value matters. Keeping every old value wastes space, but deleting by age could lose the current value.",
        "use_when": "For data that describes the current state of something, keyed by its id.",
        "steps": [
            "You set the topic's cleanup policy to compact.",
            "A background job finds keys that have newer values.",
            "It removes the older ones. A message with an empty value deletes that key.",
        ],
        "run_shows": "After writing 5 versions of 3 drivers and deleting one, only the newest version of the other 2 remains.",
    },
    "transaction-markers": {
        "problem": "A reader needs to know whether each transaction was committed or cancelled, so Kafka must store that decision in the log.",
        "use_when": "It happens by itself. It explains why offsets in a transactional topic can have small gaps.",
        "steps": [
            "A producer commits a transaction.",
            "Kafka adds a small control record to the file.",
            "Readers use it, but your program never receives it as a message.",
        ],
        "run_shows": "A table from the real file. Two messages are followed by a control marker, and readers see only the 2 messages.",
    },

    "brokers": {
        "problem": "One computer has limits, and if it fails, everything stops. Several computers solve both problems.",
        "use_when": "In every real system. Three brokers is a common starting point.",
        "steps": [
            "Several brokers join into one cluster.",
            "The controllers choose one active controller between themselves.",
            "The active controller records which brokers are alive and who leads each part.",
        ],
        "run_shows": "A table of the 3 brokers, and which one is the active controller right now.",
    },
    "replication": {
        "problem": "If a disk breaks and the data existed in one place only, it is gone forever.",
        "use_when": "Always in production. Three copies is the common choice.",
        "steps": [
            "Each part has one leader and some followers.",
            "Programs talk to the leader.",
            "Followers keep copying from the leader. Copies that are up to date are called in-sync.",
        ],
        "run_shows": "A table of ride-events showing the leader, the copies and which copies are in sync for each part.",
    },
    "failover": {
        "problem": "Computers will fail. The system must keep working, with nobody having to fix it at that moment.",
        "use_when": "It happens by itself. This test proves that it works.",
        "steps": [
            "A broker stops.",
            "The controller notices, and chooses a healthy copy as the new leader.",
            "Programs find the new leader and carry on. When the broker returns, it catches up.",
        ],
        "run_shows": "It writes for 15 seconds and counts every message. Stop a broker while it runs: nothing is lost, and the table shows which leaders moved.",
    },

    "windows": {
        "problem": "Events can arrive late or out of order. If you count by arrival time, your totals will be wrong.",
        "use_when": "For counts and totals over time, such as requests per minute.",
        "steps": [
            "Read the real time from inside each event.",
            "Put the event in the time block it belongs to.",
            "Wait a short extra time for late events, then close the block. Anything later is dropped.",
        ],
        "run_shows": "Events arrive mixed up, but land in the right 10-second blocks. One event that arrived far too late is dropped.",
    },
    "stream-table-join": {
        "problem": "A message often holds only an id. To be useful, it needs more details from somewhere else.",
        "use_when": "To add names, prices or other details to events as they pass through.",
        "steps": [
            "Keep the details in a compacted topic, so it works like a table.",
            "Read the table into memory.",
            "For each event, look up its id and add the details.",
        ],
        "run_shows": "Rides are joined with driver details. When a driver's rating changes, later rides show the new rating.",
    },
    "changelog": {
        "problem": "Totals kept in memory disappear in a crash. Starting again from all the old data could take a long time.",
        "use_when": "For any program that keeps running totals. RideFlow's dashboards use this to survive restarts.",
        "steps": [
            "Each time a total changes, also write it to a compacted topic.",
            "After a crash, memory is empty.",
            "Read the topic back, and the totals return.",
        ],
        "run_shows": "Counts are built, then thrown away as if in a crash. They are rebuilt from Kafka and match exactly.",
    },

    "sasl": {
        "problem": "Without a login, anyone who can reach a broker can read and write everything.",
        "use_when": "In every real system.",
        "steps": [
            "Users are created in the cluster.",
            "A program sends its username and proves its password when it connects.",
            "Kafka checks them, and refuses the connection if they are wrong.",
        ],
        "run_shows": "A table of three attempts: the right password works, and a wrong password and an unknown user are refused.",
    },
    "acl": {
        "problem": "Logging in proves who you are, but not what you should be able to do.",
        "use_when": "When different teams or programs share a cluster.",
        "steps": [
            "An admin writes rules, such as: alice may read and write this topic.",
            "Each time a user acts, Kafka checks the rules.",
            "Anything not allowed is refused.",
        ],
        "run_shows": "A table showing alice allowed on her topic but refused on another, and bob refused even though his password is right.",
    },
    "tls": {
        "problem": "Without encryption, anyone watching the network can read messages and passwords as they pass.",
        "use_when": "Whenever data travels over a network you do not fully control.",
        "steps": [
            "The broker shows a certificate that proves who it is.",
            "The client checks that certificate against one it trusts.",
            "Both sides agree on a secret key, and all traffic is scrambled.",
        ],
        "run_shows": "The real connection details from broker 1, a client that refuses an untrusted certificate, and a message sent through the encrypted connection.",
    },

    "schema-registry": {
        "problem": "Kafka only sees bytes. If one team changes the message format, other teams' programs can break.",
        "use_when": "When many teams share data over a long time.",
        "steps": [
            "The agreed message format is saved in the registry, with a version.",
            "A new version is checked against the old one.",
            "A change that would break readers is refused.",
        ],
        "run_shows": "A safe change is accepted, a breaking change is refused, and old data is read with the new format.",
    },
    "connect": {
        "problem": "Most systems need to move data to and from databases and files. Writing that code for each system takes a lot of work.",
        "use_when": "When a ready-made connector exists for the system you need.",
        "steps": [
            "You choose a connector, and describe it in a small settings file.",
            "You send those settings to Kafka Connect.",
            "It starts copying data, and keeps track of its own progress.",
        ],
        "run_shows": "A connector is created, and 5 messages from a topic appear as lines in a real file.",
    },
    "mirror-maker": {
        "problem": "Replication protects against a broker failure, but not against losing a whole data centre.",
        "use_when": "For disaster recovery, or to bring data closer to users in another region.",
        "steps": [
            "A second cluster runs somewhere else.",
            "MirrorMaker reads chosen topics from the first cluster.",
            "It writes them to the second cluster, with the source name added.",
        ],
        "run_shows": "A message written to the main cluster appears in the second cluster as primary.lab.mirror-demo.",
    },
    "monitoring": {
        "problem": "Without live numbers, you only learn about a problem when users complain.",
        "use_when": "In every real system, along with alerts on the important numbers.",
        "steps": [
            "An exporter reads numbers from Kafka.",
            "Prometheus collects and stores them every few seconds.",
            "Grafana draws them as graphs.",
        ],
        "run_shows": "Live numbers read from Prometheus: brokers, lag and under-replicated parts, and a check that the Grafana dashboard exists.",
    },
}
