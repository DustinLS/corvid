using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Corvid
{
    #region Corvid.Packet

    public struct Pair
    {
        private string _key;
        private string _value;

        public string Key
        {
            get { return this._key; }
            set { this._key = value; }
        }

        public string Value
        {
            get { return this._value; }
            set { this._value = value; }
        }

        public Pair(string key, string value)
        {
            this._key = key;
            this._value = value;
        }
    }

    public class PairCollection
    {
        private List<Pair> _pairs;

        public string this[string key]
        {
            get
            {
                int index = GetIndex(key);

                if (index < 0)
                {
                    return "";
                }
                else
                {
                    return this._pairs[index].Value;
                }
            }

            set
            {
                int index = GetIndex(key);

                if (index < 0)
                {
                    this._pairs.Add(new Pair(key, value));
                }
                else
                {
                    this._pairs[index] = new Pair(key, value);
                }
            }
        }

        public string this[int index]
        {
            get
            {
                if (this._pairs.Count >= index)
                {
                    return this._pairs[index].Value;
                }
                else
                {
                    return "";
                }
            }
        }

        public PairCollection()
        {
            this._pairs = new List<Pair>();
        }

        public int Count()
        {
            return this._pairs.Count;
        }

        private int GetIndex(string key)
        {
            int count = this._pairs.Count;

            if (count < 0)
            {
                return -1;
            }

            for (int i = 0; i < count; i++)
            {
                if (this._pairs[i].Key == key)
                {
                    return i;
                }
            }

            return -1;
        }
    }

    public class Header
    {
        public int Length { get; set; }
        public Service Service { get; set; }
        public Status Status { get; set; }

        public Header()
        {
            this.Service = Service.Default;
            this.Status = Status.Default;
        }

        public Header(int service, int status, params string[] data)
        {
            this.Service = (Service)service;
            this.Status = (Status)status;
        }

        public Header(Service service, Status status, params string[] data)
        {
            this.Service = service;
            this.Status = status;
        }

        public byte[] ToArray()
        {
            return ToArray(this.Status);
        }

        public byte[] ToArray(Status status)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                // Header
                ms.Write(new byte[] { 0x01, 0x13, 0x37 }, 0, 3);

                // Service
                ms.WriteByte((byte)this.Service);
                
                // Status
                ms.WriteByte((byte)status);
                
                // Get payload length and contents
                ms.Write(BitConverter.GetBytes(Convert.ToInt16(this.Length)).Take(2).Reverse().ToArray(), 0, 2);
                
                // End Of Transmission
                ms.Write(new byte[] { 0x04 }, 0, 1);

                return ms.ToArray();
            }
        }

        public static bool TryParse(byte[] data, ref Packet packet)
        {
            packet = null;

            // Validate data
            if (data.Count() < 7)
            {
                return false;
            }

            if (data[0] != 0x01 && data[1] != 0x13 && data[2] != 0x37)
            {
                return false;
            }

            // Process header
            packet = new Packet();
            packet.Service = (Service)data[3];
            packet.Status = (Status)data[4];
            packet.Length = BitConverter.ToInt16(new byte[] { data[6], data[5] }, 0);

            return true;
        }
    }

    public class Packet
    {
        public List<string> Data { get; set; }
        public int Length { get; set; }
        public Service Service { get; set; }
        public Status Status { get; set; }

        public int Count
        {
            get { return this.Data.Count; }
        }

        public Packet()
        {
            this.Data = new List<string>();
            this.Service = Service.Default;
            this.Status = Status.Default;
        }

        public Packet(int service, int status, params string[] data)
        {
            this.Service = (Service)service;
            this.Status = (Status)status;
            this.Data = new List<string>();
            this.Data = data.ToList();
        }

        public Packet(Service service, Status status, params string[] data)
        {
            this.Service = service;
            this.Status = status;
            this.Data = data.ToList();
        }

        public static bool TryParse(byte[] data, ref Packet packet)
        {
            packet = null;

            // Validate data
            if (data.Count() < 7)
            {
                return false;
            }

            if (data[0] != 0x01 && data[1] != 0x13 && data[2] != 0x37)
            {
                return false;
            }

            // Process header
            packet = new Packet();
            packet.Service = (Service)data[3];
            packet.Status = (Status)data[4];
            packet.Length = BitConverter.ToInt16(new byte[] { data[6], data[5] }, 0);

            // Validate contents
            if (data.Count() < (packet.Length + 7))
            {
                return false;
            }

            // Process contents
            if (packet.Length > 0 && data.Count() >= (packet.Length + 7))
            {
                packet.Data = new List<string>();

                for (int i = 7; i < (packet.Length + 7); i++)
                {
                    if (data[i] == 0x1f)
                    {
                        string value = "";

                        do
                        {
                            i++;
                            value += (char)data[i];
                        } while (data[i + 1] != 0x1f && data[i + 1] != 0x04);

                        packet.Data.Add(value);
                    }
                }
            }

            return true;
        }

        public byte[] ToArray()
        {
            return ToArray(this.Status);
        }

        public byte[] ToArray(Status status)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                // Header
                ms.Write(new byte[] { 0x01, 0x13, 0x37 }, 0, 3);

                // Service
                ms.WriteByte((byte)this.Service);

                // Status
                ms.WriteByte((byte)status);

                // Get payload length and contents
                int length = 0;

                MemoryStream contents = null;

                try
                {
                    contents = new MemoryStream();

                    for (int i = 0; i < this.Count; i++)
                    {
                        contents.Write(new byte[] { 0x1f }, 0, 1);
                        contents.Write(Encoding.GetEncoding(1252).GetBytes(this.Data[i]), 0, this.Data[i].Length);
                        length = length + (this.Data[i].Length + 1);
                    }

                    // Payload length
                    this.Length = (length + 1);

                    ms.Write(BitConverter.GetBytes(Convert.ToInt16(length + 1)).Take(2).Reverse().ToArray(), 0, 2);

                    // Payload contents
                    ms.Write(contents.ToArray(), 0, length);

                    // End Of Transmission
                    ms.Write(new byte[] { 0x04 }, 0, 1);
                }
                finally
                {
                    if (contents != null)
                    {
                        contents.Close();
                    }
                }

                return ms.ToArray();
            }
        }

        public static implicit operator List<string>(Packet packet)
        {
            return packet.Data;
        }
    }

    public enum Service
    {
        Default,
        Ping,
        Get,
        Set,
        Delete
    }

    public enum Status
    {
        Default = 0x00,
        Query = 0x05,
        Acknowledge = 0x06,
        DataLink = 0x10,
        Negative = 0x15,
        Sync = 0x16
    }

    public enum DataSeparator
    {
        File = 0x1c,
        Group = 0x1d,
        Record = 0x1e,
        Unit = 0x1f
    }

    #endregion

    #region Corvid.EventHandler

    /// <summary>
    /// Request delegate for server event handling
    /// </summary>
    /// <param name="client"></param>
    public delegate void RequestHandler(object client, EventArgs e);

    /// <summary>
    /// Request event arguments
    /// </summary>
    public class RequestEventArgs : EventArgs
    {
        public Packet Packet { get; set; }

        public RequestEventArgs(Packet packet)
        {
            this.Packet = packet;
        }
    }

    #endregion

    #region Corvid.Server

    /// <summary>
    /// Manages server that listens for clients and processes requests
    /// </summary>
    public class Server
    {
        private TcpListener _serverSocket;
        private List<TcpClient> _serverClients;

        public bool Active { get; set; }

        public event RequestHandler Request;

        public Server()
        {
            try
            {
                this._serverClients = new List<TcpClient>();
                this._serverSocket = new TcpListener(IPAddress.Any, 9001);
                this._serverSocket.Start();
                this.Active = true;
            }
            catch
            {

            }
        }

        public Server(string address, int port)
        {
            try
            {
                this._serverClients = new List<TcpClient>();
                this._serverSocket = new TcpListener(IPAddress.Parse(address), port);
                this._serverSocket.Start();
                this.Active = true;
            }
            catch
            {

            }
        }

        /// <summary>
        /// Check if client sockets have sent data,
        /// process buffers if data is waiting.
        /// </summary>
        public void Poll()
        {
            try
            {
                if (!this.Active)
                {
                    return;
                }

                if (this._serverSocket.Pending())
                {
                    TcpClient client = this._serverSocket.AcceptTcpClient();

                    if (client != null)
                    {
                        this._serverClients.Add(client);
                    }
                    else
                    {
                        // Could not accept connection
                    }
                }
                else
                {
                    int clients = this._serverClients.Count;

                    if (clients > 0)
                    {
                        for (int i = 0; i < clients; i++)
                        {
                            if (this._serverClients[i].Connected)
                            {
                                DateTime ttl = DateTime.Now.AddMilliseconds(500);

                                while (this._serverClients[i].Available > 0)
                                {
                                    ProcessPackets(this._serverClients[i]);

                                    if (DateTime.Now > ttl)
                                    {
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                this._serverClients.RemoveAt(i);

                                break;
                            }
                        }
                    }
                }
            }
            catch
            {

            }
        }

        private void ProcessPackets(TcpClient client)
        {
            try
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    if (!this.Active)
                    {
                        return;
                    }

                    if (client.Available < 7)
                    {
                        return;
                    }

                    // Get stream
                    DateTime ttl;
                    Packet packet = null;
                    NetworkStream stream = client.GetStream();

                    // Receive header
                    //
                    //   Buffer length must be at least 7 bytes to get here, if bytes received are
                    //   an invalid header, send a NEG packet to client as response

                    byte[] buffer = new byte[7];
                    int recv = stream.Read(buffer, 0, 7);

                    ms.Write(buffer, 0, recv);

                    if (!(recv > 0 && Header.TryParse(ms.ToArray(), ref packet)))
                    {
                        Send(client, new Packet(Service.Default, Status.Negative, "-1"));
                        return;
                    }

                    // Process contents
                    ttl = DateTime.Now.AddMilliseconds(2500);

                    while (DateTime.Now < ttl)
                    {
                        if (client.Available >= packet.Length)
                        {
                            buffer = new byte[packet.Length];
                            recv = stream.Read(buffer, 0, packet.Length);
                            ms.Write(buffer, 0, recv);

                            if (recv > 0)
                            {
                                if (Packet.TryParse(ms.ToArray(), ref packet))
                                {
                                    if (Request != null)
                                    {
                                        Request(client, new RequestEventArgs(packet));
                                    }

                                    return;
                                }
                            }
                        }
                    }

                    this.Send(client, new Packet(packet.Service, Status.Negative, "-1"));
                }
            }
            catch
            {

            }
        }

        /// <summary>
        /// Sends data to client socket.
        /// </summary>
        public void Send(int index, Packet packet)
        {
            try
            {
                if (index < _serverClients.Count)
                {
                    Send(this._serverClients[index], packet.ToArray());
                }
            }
            catch
            {

            }
        }

        public void Send(object client, Packet packet)
        {
            try
            {
                Send((TcpClient)client, packet.ToArray());
            }
            catch
            {

            }
        }

        public void Send(TcpClient client, Packet packet)
        {
            try
            {
                Send(client, packet.ToArray());
            }
            catch
            {

            }
        }

        public void Send(TcpClient client, byte[] buffer)
        {
            try
            {
                if (client.Connected)
                {
                    client.GetStream().Write(buffer, 0, buffer.Count());
                }
            }
            catch
            {
                // Nothing to do here.
            }
        }

        public void Stop()
        {
            try
            {
                if (this.Active)
                {
                    // Set active flag to false
                    this.Active = false;

                    // Close listening socket
                    this._serverSocket.Stop();

                    // Close client sockets
                    int clients = _serverClients.Count;

                    for (int i = 0; i < clients; i++)
                    {
                        this._serverClients[i].GetStream().Close();
                        this._serverClients[i].Close();
                    }
                }
            }
            catch
            {

            }
        }
    }

    #endregion

    #region Corvid.Client

    public class Client
    {
        private TcpClient _clientSocket = null;

        public event RequestHandler Request;
        public bool Ping { get; set; }
        public string Name { get; set; }

        public Client(string hostname, int port)
        {
            this.Name = "";
            this.Ping = false;
            this._clientSocket = new TcpClient();
            this._clientSocket.Connect(hostname, port);
        }

        /// <summary>
        /// Check if client sockets have sent data,
        /// process buffers if data is waiting.
        /// </summary>
        public void Poll()
        {
            DateTime ttl = DateTime.Now.AddMilliseconds(500);

            while (this._clientSocket.Available > 0)
            {
                ProcessPackets(this._clientSocket);

                if (DateTime.Now > ttl)
                {
                    break;
                }
            }
        }

        private void ProcessPackets(TcpClient client)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                if (client.Available < 7)
                {
                    return;
                }

                // Get stream
                DateTime ttl;
                Packet packet = null;
                NetworkStream stream = client.GetStream();

                // Receive header
                //
                //   Buffer length must be at least 7 bytes to get here, if bytes received are
                //   an invalid header, send a NEG packet to client as response

                byte[] buffer = new byte[7];
                int recv = stream.Read(buffer, 0, 7);

                ms.Write(buffer, 0, recv);

                if (!(recv > 0 && Header.TryParse(ms.ToArray(), ref packet)))
                {
                    Send(new Packet(Service.Default, Status.Negative, "-1"));
                    return;
                }

                // Process contents
                ttl = DateTime.Now.AddMilliseconds(2500);

                while (DateTime.Now < ttl)
                {
                    if (client.Available >= packet.Length)
                    {
                        buffer = new byte[packet.Length];
                        recv = stream.Read(buffer, 0, packet.Length);
                        ms.Write(buffer, 0, recv);

                        if (recv > 0)
                        {
                            if (Packet.TryParse(ms.ToArray(), ref packet))
                            {
                                if (this.Ping)
                                {
                                    ProcessPing(packet);
                                }

                                if (Request != null)
                                {
                                    Request(client, new RequestEventArgs(packet));
                                }

                                return;
                            }
                        }
                    }
                }

                Send(new Packet(packet.Service, Status.Negative, "-1"));
            }
        }

        private void ProcessPing(Packet packet)
        {
            if (packet.Service == Service.Ping)
            {
                if (packet.Status == Status.Query)
                {
                    Send(new Packet(Service.Ping, Status.Acknowledge, this.Name));
                }
            }
        }

        public void Send(Packet packet)
        {
            Send(packet.ToArray());
        }

        public void Send(byte[] buffer)
        {
            try
            {
                this._clientSocket.GetStream().Write(buffer, 0, buffer.Count());
            }
            catch
            {
                // nothing to do here
            }
        }

        public void Send(byte[] buffer, int offset, int size)
        {
            try
            {
                this._clientSocket.GetStream().Write(buffer, offset, size);
            }
            catch
            {
                // nothing to do here
            }
        }
    }

    #endregion
}
