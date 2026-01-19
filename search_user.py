import sqlite3

conn = sqlite3.connect("users4.db")
cur = conn.cursor()

print("🔍 Mobile Number Search System")
print("Type 'exit' to quit\n")

while True:
    mobile = input("Enter mobile number: ").strip()

    if mobile.lower() in ("exit", "quit", "q"):
        print("\n👋 Exiting program. Goodbye!")
        break

    cur.execute("""
    SELECT mobile, name, fname, address, alt, circle, id, email
    FROM users
    WHERE mobile = ?
    """, (mobile,))

    row = cur.fetchone()

    if row:
        print("\n✅ MATCH FOUND\n")
        print("Mobile :", row[0])
        print("Name   :", row[1])
        print("Fname  :", row[2])
        print("Address:", row[3])
        print("Alt    :", row[4])
        print("Circle :", row[5])
        print("ID     :", row[6])
        print("Email  :", row[7] if row[7] else "None")
    else:
        print("\n❌ No record found")

    print("-" * 40)

conn.close()
