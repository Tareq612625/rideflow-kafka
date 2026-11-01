"""Beginner explanations for the Feature Lab, kept in one place so they are easy to edit.

Every idea uses the same picture: Kafka is a shared notebook. BASICS teaches the
words once. EASY explains each feature with no jargon, plus an everyday example.
"""

BASICS = [
    {"word": "Message", "meaning": "One small note about something that happened, such as: trip 101 was completed."},
    {"word": "Topic", "meaning": "A notebook with a name. All notes of one kind go in it, such as ride-events."},
    {"word": "Partition", "meaning": "One notebook divided into smaller parts, so many people can work at the same time."},
    {"word": "Offset", "meaning": "The line number of a note. The first note in each part is line 0."},
    {"word": "Key", "meaning": "A label on a note, such as a driver's ID. Notes with the same label go to the same part."},
    {"word": "Producer", "meaning": "A program that writes notes into Kafka."},
    {"word": "Consumer", "meaning": "A program that reads notes. Reading does not erase them."},
    {"word": "Consumer group", "meaning": "A team of readers who share the work between them."},
    {"word": "Commit", "meaning": "A bookmark. It saves where a reader stopped."},
    {"word": "Lag", "meaning": "How many notes are still waiting to be read."},
    {"word": "Broker", "meaning": "One computer that runs Kafka and stores the notebooks. We have 3."},
    {"word": "Replica", "meaning": "A photocopy of a part, kept on another broker in case one breaks."},
]

# feature id -> (what it means in simple words, an everyday example)
EASY = {
    "topic-admin": (
        "A topic is a named notebook. This creates one, gives it more parts, changes one of its rules, then deletes it.",
        "Like opening a new notebook for a new subject, adding more sections, then throwing it away when you finish.",
    ),
    "offsets": (
        "Messages go into different parts of a topic. In each part they get line numbers: 0, 1, 2, and so on, with no gaps.",
        "Like roll numbers. Every class has its own list, and every list starts from the beginning.",
    ),
    "keys": (
        "All messages with the same key go to the same part, so they stay in the order they were sent.",
        "Like all letters for one person going into one mailbox. They stay in the order they arrived.",
    ),
    "no-key": (
        "A message with no key can go to any part. The work spreads evenly, but the messages have no fixed order.",
        "Like customers in a shop going to whichever cash counter is free.",
    ),
    "custom-partitioner": (
        "Normally Kafka chooses the part for you. Here we write our own rule: VIP drivers always go to part 0.",
        "Like a bank with a special counter for VIP customers.",
    ),
    "headers": (
        "A header is a small label stuck on a message. It gives extra information without touching the message itself.",
        "Like writing Fragile on a parcel. You can read it without opening the parcel.",
    ),
    "timestamps": (
        "Every message stores a time. It can be the time the sender gave, or the time Kafka really received it.",
        "Like a letter: the date the writer put on it, and the stamp the post office adds when it arrives.",
    ),
    "message-size": (
        "Every topic has a size limit for one message. A bigger message is refused.",
        "Like a courier that refuses a parcel over the weight limit. For a big file, send a link to it.",
    ),

    "acks": (
        "acks means: how much confirmation do you wait for after sending? 0 means none, 1 means the main broker has it, all means every healthy copy has it.",
        "Like posting a letter: drop it and walk away (0), get a receipt at the counter (1), or wait until every office confirms (all).",
    ),
    "min-insync": (
        "You can say: accept a message only if enough copies are healthy. If too few are healthy, Kafka says no instead of risking your data.",
        "Like a bank cheque that needs 3 signatures. If one person is absent, the cheque is refused.",
    ),
    "batching": (
        "Instead of sending messages one by one, the producer packs many together and sends them at once. This is much faster.",
        "One bus carrying 40 people, instead of 40 separate cars.",
    ),
    "compression": (
        "Messages are squeezed smaller before they are sent. This saves disk and network, and the reader unpacks them automatically.",
        "Like putting files into a ZIP before sending them.",
    ),
    "idempotence": (
        "Sometimes a message is saved but the reply gets lost, so the sender sends it again. This feature stops the second copy from being stored.",
        "You press Pay, the page freezes, so you press again. The shop sees the same order number and charges you only once.",
    ),
    "quota": (
        "A speed limit for one program, so that it cannot use up the whole system.",
        "Like a Wi-Fi limit on one user, so that everyone else still gets internet.",
    ),

    "group-share": (
        "Several readers join one team. Kafka gives each reader different parts, so the work is shared and nothing is read twice.",
        "Three delivery riders, each with their own area.",
    ),
    "rebalance": (
        "If one reader leaves the team, Kafka gives its work to the others automatically.",
        "One rider goes home sick, so the other riders take over that area.",
    ),
    "pub-sub": (
        "Two different teams can read the same messages. Each team gets all of them, and they do not disturb each other.",
        "Everyone can watch the same TV channel. One viewer does not use up the programme.",
    ),
    "assignors": (
        "When a team shares work, there are different rules for dividing it. Some rules are more even than others.",
        "Dividing 6 tasks between 2 people: one way gives 4 and 2, a fairer way gives 3 and 3.",
    ),
    "static-membership": (
        "Give a reader a fixed name. If it restarts quickly, Kafka waits for it instead of reorganising the whole team.",
        "A staff member with a fixed ID goes for tea. The manager does not reassign everyone's work.",
    ),
    "offset-reset": (
        "When a brand new reader joins, where should it start? From the oldest message (earliest), only new ones (latest), or stop with an error (none).",
        "Starting a TV series: watch from episode 1, or start from tonight's episode.",
    ),
    "delivery": (
        "If a program crashes in the middle of its work, messages can be lost or repeated. The order of doing the work and saving your place decides which one.",
        "Reading a book: mark the page before reading and fall asleep, and you skip pages. Read first and mark later, and you may read some again.",
    ),
    "seek": (
        "A reader can jump straight to any message, by line number or by time.",
        "Like jumping to any minute of a YouTube video.",
    ),
    "reset-offsets": (
        "You can move a team's bookmark backwards, so it reads old messages again.",
        "Going back a few pages after you realise you misunderstood the chapter.",
    ),
    "pause-resume": (
        "A reader can stop reading one part for a while, and keep reading the others.",
        "A cashier closes one queue for a short break while the other queues keep moving.",
    ),
    "lag": (
        "Lag is how many messages are waiting to be read. If it keeps growing, the reader is too slow.",
        "Orders waiting in a restaurant kitchen. If they keep piling up, the cooks are falling behind.",
    ),
    "dead-letter": (
        "A broken message is moved to a separate place, so it does not stop all the good messages behind it.",
        "A post office puts a letter with an unreadable address in a special box, and keeps delivering the rest.",
    ),
    "retry-topic": (
        "If a job fails for a short time, put it aside and try again later, while other work continues.",
        "A customer's phone is busy, so you call back in 5 minutes and serve the next customer meanwhile.",
    ),

    "txn-commit-abort": (
        "A transaction means: all of these messages are saved together, or none of them count.",
        "A bank transfer: money leaves one account and enters the other, or nothing happens at all.",
    ),
    "isolation": (
        "A careful reader waits until a transaction is finished. It never sees unfinished work.",
        "Exam results are published only after all the checking is finished.",
    ),
    "exactly-once": (
        "Even if a program crashes and tries again, each result counts exactly one time. Nothing is lost and nothing is repeated.",
        "A teacher writes the mark and ticks the paper in one action. After a break, no paper is skipped or marked twice.",
    ),
    "pipeline-audit": (
        "This checks our real billing system. It compares every completed trip with every invoice, one by one.",
        "Like an accountant matching every receipt against the sales book.",
    ),
    "fencing": (
        "If an old copy of a program wakes up after a new copy has taken over, Kafka blocks the old one.",
        "When a new manager gets the office key, the old key stops working.",
    ),

    "segments": (
        "Kafka stores messages in normal files on disk. When a file gets big or old, it starts a new one.",
        "When a notebook is full, you start a new one.",
    ),
    "retention": (
        "Kafka deletes old messages after a set time, even if nobody read them. The default is 7 days.",
        "CCTV that keeps footage for 7 days, then records over it.",
    ),
    "delete-records": (
        "You can delete old messages yourself, right now, up to a line number you choose.",
        "Tearing out the first 5 pages of a notebook.",
    ),
    "compaction": (
        "Kafka keeps only the newest value for each key, and removes the older ones.",
        "Your phone keeps only a friend's latest number, not every number they ever had.",
    ),
    "transaction-markers": (
        "When a transaction finishes, Kafka writes a small hidden mark in the file saying saved or cancelled. It uses up one line number.",
        "An Approved stamp at the end of a form. It is on the page, but it is not part of what you wrote.",
    ),

    "brokers": (
        "A broker is one computer running Kafka. We have 3 working together. One of them also acts as the manager, called the controller.",
        "Three branch offices, with one acting as head office. If it fails, another takes over.",
    ),
    "replication": (
        "Each part is stored on 3 brokers. One is the leader that does the work, and the others keep copies.",
        "Keeping your photos on your phone, your laptop and Google Drive.",
    ),
    "failover": (
        "If one broker stops, another broker with a copy takes over automatically, and no data is lost.",
        "The main cashier leaves, and an assistant with the same records takes over at once.",
    ),

    "windows": (
        "Count events in time blocks, such as every 10 seconds, using the time each event really happened. Events that arrive far too late are ignored.",
        "Counting shop customers per hour: someone who came at 9:50 counts in that hour, even if you hear about it at 10:05.",
    ),
    "stream-table-join": (
        "Add extra details to a message by looking them up. A ride has only a driver ID, and we add the driver's name and rating.",
        "A cashier scans a barcode, and the system adds the product's name and price.",
    ),
    "changelog": (
        "A program keeps its counts in memory, and also writes each change to Kafka. After a crash, it reads them back and carries on.",
        "Keeping a diary of your savings. If you forget the total, the diary gives it back.",
    ),

    "sasl": (
        "Programs must log in with a username and password before they can use Kafka.",
        "Like logging in to Facebook.",
    ),
    "acl": (
        "After logging in, each user can only do what they are allowed to do. Alice can use one topic, and Bob can use nothing.",
        "An office ID card opens only the doors you are allowed through.",
    ),
    "tls": (
        "Messages are scrambled while they travel over the network, so nobody in the middle can read them.",
        "A sealed envelope instead of a postcard. It is the same idea as the padlock in your browser.",
    ),

    "schema-registry": (
        "It keeps the agreed format of your messages, and refuses a change that would break the programs reading them.",
        "A form with fixed fields. You may add an optional field, but not a required one that old forms do not have.",
    ),
    "connect": (
        "It moves data between Kafka and other systems without you writing code. Here it copies a topic into a file.",
        "A ready-made pipe between two water tanks.",
    ),
    "mirror-maker": (
        "It copies messages to a second Kafka cluster in another place.",
        "Keeping a copy of important papers in another city, in case of a fire.",
    ),
    "monitoring": (
        "Live graphs that show Kafka's health, so problems are seen early.",
        "A car dashboard showing your speed and fuel.",
    ),
}
