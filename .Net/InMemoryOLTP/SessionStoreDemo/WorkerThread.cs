/****************************************************************************/
/*                       Expert SQL Server In-Memory OLTP                   */
/*      APress. 1st Edition. ISBN-13:978-1484211373  ISBN-10:1484211375     */
/*                                                                          */
/*           Written by Dmitri V. Korotkevitch & Vladimir Zatuliveter       */
/*                      http://aboutsqlserver.com                           */
/*                        dk@aboutsqlserver.com                             */
/****************************************************************************/
/*                     Chapter 11 - Session Store Demo                      */
/****************************************************************************/

/******************************************************************************/
/* This is oversimplified example to illustrate the basic concepts. Production *
 * implementation should have additional code to resolve concurrency conflicts *
 /******************************************************************************/


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data.SqlClient;
using System.Data;
using AboutSqlServer.Com.Classes;

namespace Actsoft.Com.SessionStoreDemo
{
    public  class WorkerThread : BaseThread
    {
        [Serializable]
        class SessionObject
        {
            public byte[] data;
            public int iteration;

            public SessionObject(int objSize)
            {
                data = Enumerable.Repeat<byte>(0,objSize).ToArray<byte>();
                iteration = 0;
            }
        }

        public WorkerThread(ConnectionManager connManager, bool useInMemOLTP, bool useTVP, int objSize, int iterations) : base(0) 
        {
            this._connManager = connManager;
            this._obj = new SessionObject(objSize);
            this._useTVP = useTVP || (objSize > 7800);
            this._useInMemOLTP = useInMemOLTP;
            this._iterations = iterations;
        }

        protected override IDisposable GetExecuteDisposable()
        {
            _conn = _connManager.GetConnection();
            return _conn;
        }

        protected override void OnExecute()
        {
            base.OnExecute();
            CreateCommands(_conn);
        }

        private void CreateCommands(SqlConnection conn)
        {
            _cmdLoad = new SqlCommand("dbo.LoadObjectFromStore" + (_useInMemOLTP ? String.Empty : "_Disk"), conn);
            _cmdLoad.CommandType = System.Data.CommandType.StoredProcedure;
            _cmdLoad.Parameters.Add("@ObjectKey", SqlDbType.UniqueIdentifier);

            _cmdSave = new SqlCommand("dbo.SaveObjectToStore" +
                    (_useTVP?String.Empty:"_Row") + 
                    (_useInMemOLTP ? String.Empty : "_Disk"), conn);
            _cmdSave.CommandType = System.Data.CommandType.StoredProcedure;
            _cmdSave.Parameters.Add("@ObjectKey", SqlDbType.UniqueIdentifier);
            _cmdSave.Parameters.Add("@ExpirationTime", SqlDbType.DateTime2).Value = DateTime.UtcNow.AddHours(1);
            if (_useTVP)
                _cmdSave.Parameters.Add("@ObjData", SqlDbType.Structured).TypeName = "dbo.tvpObjData" + (_useInMemOLTP ? String.Empty : "_Disk");
            else
                _cmdSave.Parameters.Add("@ObjData", SqlDbType.VarBinary, 8000);

        }

        protected sealed override void DoIteration()
        {
            if (_iteration % _iterations == 1)
            { // Emulating new session
                _objectKey = Guid.NewGuid();
                _cmdLoad.Parameters[0].Value = _cmdSave.Parameters[0].Value = _objectKey;
            }
            else
            { // Loading data from db
                // Step 1: Getting serialized chunks
                _cmdLoad.Parameters[0].Value = _objectKey;
                var chunks = new List<byte[]>();
                using (var reader = _cmdLoad.ExecuteReader())
                {
                    while (reader.Read())
                        chunks.Add((byte[])reader[0]);
                }
                if (chunks.Count == 0)
                    throw new Exception("Cannot locate an object with key: " + _objectKey.ToString());
                // Step 2: Deserializing
                SessionObject loadedObj = ObjStoreUtils.Deserialize<SessionObject>(ObjStoreUtils.Merge(chunks));
                // Validation - for demo purposes
                if (loadedObj.iteration != _obj.iteration)
                    throw new Exception("Validation failed: Iterations do not match");
            }
            // Saving object to DB
            _obj.iteration = _iteration;
            byte[] serializedObj = ObjStoreUtils.Serialize<SessionObject>(_obj);
            if (_useTVP)
            {
                DataTable tbl = new DataTable();
                tbl.Columns.Add("ChunkNum", typeof(short));
                tbl.Columns.Add("Data", typeof(byte[]));
                var chunks = ObjStoreUtils.Split(serializedObj, 8000);
                for (int i = 0; i < chunks.Count; i++)
                    tbl.Rows.Add(i + 1, chunks[i]);
                _cmdSave.Parameters[2].Value = tbl;
            }
            else
                _cmdSave.Parameters[2].Value = serializedObj;
            _cmdSave.ExecuteNonQuery();
        }

        private ConnectionManager _connManager;
        private SqlConnection _conn;
        private SqlCommand _cmdLoad;
        private SqlCommand _cmdSave;
        private SessionObject _obj;
        private int _iterations;
        private bool _useTVP;
        private bool _useInMemOLTP;
        private Guid _objectKey;
    }
}
