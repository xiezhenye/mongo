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
#include <stdint.h>
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>

#include "mongo/base/initializer.h"
#include "mongo/db/json.h"
#include "mongo/db/repl/oplogreader.h"
#include "mongo/tools/tool.h"
#include "mongo/util/text.h"

using namespace mongo;

namespace po = boost::program_options;

class OplogDumpTool : public Tool {
public:
    OplogDumpTool() : Tool( "oplog", NONE ) {
        add_options()
        ("begin,b" , po::value<int>() , "time to dump begin" )
        ("end,e", po::value<int>(), "time to dump end")
        ("host", po::value<string>()->default_value("localhost") , "host to pull from" )
        ("oplogns", po::value<string>()->default_value( "local.oplog.rs" ) , "ns to pull from" )
        ("out,o", po::value<string>() , "output file" )
        ;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Pull and replay a remote MongoDB oplog.\n" << endl;
    }

    int run() {
        if ( ! hasParam( "host" ) ) {
            log() << "need to specify --host" << endl;
            return -1;
        }
        if ( ! hasParam( "out" ) ) {
            log() << "need to specify --out" << endl;
            return -1;
        }

        //Client::initThread( "oplogreplay" );


        OplogReader r(false);
        //r.setTailingQueryOptions( QueryOption_SlaveOk | QueryOption_AwaitData );  | QueryOption_OplogReplay
        unsigned start_time = getParam( "begin" , 0 );
        OpTime start( start_time , 0 );
        log() << "starting from " << start_time << " " << start.toStringPretty() << endl;

        string ns = getParam( "oplogns" );

        boost::filesystem::path out = getParam( "out" );
        FILE* file = fopen( out.string().c_str() , "wb" );
        if (!file) {
            log() << "error opening file: " << out.string() << " " << errnoWithDescription() << endl;
            return -1;
        }

        log() << "going to connect" << endl;
        r.connect( getParam( "host" ) );
        log() << "connected" << endl;

        r.tailingQueryGTE( ns.c_str() , start );
        size_t bytesWriten;
        unsigned long i = 0;
        log() << "dumping..." << endl;
        while ( r.more() ) {
            BSONObj o = r.next();

            if ( o["$err"].type() ) {
                log() << "error getting oplog" << endl;
                log() << o << endl;
                return -1;
            }
            bytesWriten = fwrite(o.objdata(), sizeof(char), o.objsize(), file);
            if (bytesWriten < (size_t)o.objsize()) {
                log() << "error write file " << ferror(file) << endl;
                return -1;
            }
            i++;
            if (i % 10000 == 0) {
                cout<<'.';
                cout.flush();
            }
            if (i % (10000 * 50) == 0) {
                cout<< '\t'<<i << endl;
            }

        }
        fclose(file);
        log() << "finished." << endl;
        return 0;
    }
};

int toolMain( int argc , char** argv, char** envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    OplogDumpTool t;
    return t.main( argc , argv );
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

