/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include <boost/program_options.hpp>
#include <fcntl.h>

#include "mongo/base/initializer.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/tools/tool.h"
#include "mongo/util/mmap.h"
#include "mongo/util/text.h"

using namespace mongo;

namespace po = boost::program_options;

class OplogRestore : public BSONTool {

    enum OutputType { JSON , DEBUG } _type;

public:

    OplogRestore () : BSONTool( "oplogrestore" ) {
        add_options()
        ;
        add_hidden_options()
        ("file" , po::value<string>() , ".bson file" )
        ;
        addPositionArg( "file" , 1 );
        //_noconnection = true;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "restore dumped oplog\n" << endl;
        out << "usage: " << _name << " [options] <bson filename>" << endl;
    }

    virtual int doRun() {
        boost::filesystem::path root = getParam( "file" );
        if ( root == "" ) {
            printExtraHelp(cout);
            return 1;
        }
        Client::initThread( "oplogreplay" );
        processFile( root );
        return 0;
    }

    virtual void gotObject( const BSONObj& o ) {
        if ( o["op"].String() == "n" )
            return;

        BSONObjBuilder b( o.objsize() + 32 );
        BSONArrayBuilder updates( b.subarrayStart( "applyOps" ) );
        updates.append( o );
        updates.done();

        BSONObj c = b.obj();
            
        BSONObj res;
        bool ok = conn().runCommand( "admin" , c , res );
        if (!ok) {
            log() << "reply " << res << " failed!" << endl;
        }
    }
};

int toolMain( int argc , char ** argv, char **envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    OplogRestore oplogRestore;
    return oplogRestore.main( argc , argv );
}

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF-16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables toolMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = toolMain(argc, wcl.argv(), wcl.envp());
    ::_exit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = toolMain(argc, argv, envp);
    ::_exit(exitCode);
}
#endif
