/*
 *  Copyright (c) 2016 AT&T Labs Research
 *  All rights reservered.
 *  
 *  Licensed under the GNU Lesser General Public License, version 2.1; you may
 *  not use this file except in compliance with the License. You may obtain a
 *  copy of the License at:
 *  
 *  https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 *  
 *
 *  Author: Pingkai Liu (pingkai@research.att.com)
 */

package main


import (
    "log"
    "os"
    "io"
    "time"
    "strconv"
    "errors"
    "github.com/minio/minio-go"
    "encoding/json"
    "io/ioutil"
)


/* handle fatal errors , basically when anything goes wrong just quit 
 * and left a status code and message.
 * Notice in case of backup/restore plugin, almost every error is fatal.
 */

func do_exit (e error) {
    if e != nil {
        ioutil.WriteFile("meta/status_msg", []byte(e.Error()),  0600);
        ioutil.WriteFile("meta/status", []byte("1"),  0600);
        log.Fatalln(e);
    } else {
        ioutil.WriteFile("meta/status_msg", []byte("success"),  0600);
        ioutil.WriteFile("meta/status", []byte("0"),  0600);
        os.Exit(0);
    }
}

func main() {

    /* set up log file */
    log_file, err := os.OpenFile("meta/log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600);
    if (err != nil) {
        do_exit(err);
    }
    defer log_file.Close();
    log.SetOutput(log_file);
    /* get parameters */
    arg_buf, err := ioutil.ReadFile("meta/arg");
    if (err != nil) {
        do_exit(err);
    }
    var snpy_arg interface{};
    err = json.Unmarshal(arg_buf, &snpy_arg);
    if (err != nil) {
        do_exit(err);
    }
    var s3_arg map[string]interface{};

    s3_arg = (snpy_arg.(map[string]interface{}))["tp_param"].(map[string]interface{});
    log.Println(s3_arg["url"].(string)); 
    s3Client, err := minio.NewWithRegion(
                        s3_arg["url"].(string),
                        s3_arg["user"].(string),
                        s3_arg["password"].(string),
                        false,
                        s3_arg["regions"].(string));
    if err != nil {
        do_exit(err);
    }

    /* get/put? */
    cmd, err := ioutil.ReadFile("meta/cmd");
    if err != nil {
        do_exit(err);
    }

    if (string(cmd) == "put") {
        /* fine the export file */
        data_files, err := ioutil.ReadDir("./data");
        if err != nil {
            do_exit(err);
        }

        file, err := os.Open("data/"+data_files[0].Name());
        if err != nil {
            do_exit(err);
        }
        defer file.Close();

        fileStat, err := file.Stat();
        if err != nil {
            do_exit(err);
        }
        var start_time = float64(time.Now().Unix());
        /* set the upload key  */
        key := data_files[0].Name();
        n, err := s3Client.PutObject(s3_arg["container"].(string), key, file,
                                     fileStat.Size(),
                                     minio.PutObjectOptions{ContentType:"application/octet-stream"});
        if (err != nil) {
            do_exit(err);
        }
        log.Println("successfully uploaded: ", n);
        var fin_time = float64(time.Now().Unix());

        /* update the start and finish time in  meta/arg.out file*/
        s3_arg["put_start"] = start_time;
        s3_arg["put_fin"] = fin_time;
        snpy_arg_out_buf, err := json.Marshal(snpy_arg);
        if (err != nil) {
            do_exit(err);
        }
        err = ioutil.WriteFile("meta/arg.out", snpy_arg_out_buf, 0600);
        if (err != nil) {
            do_exit(err);
        }
        do_exit(nil);

    } else if (string(cmd) == "get") {
        rstr_arg_buf, err := ioutil.ReadFile("meta/rstr_arg");
        if (err != nil) {
            do_exit(err);
        }
        var rstr_arg map[string]interface{};
        json.Unmarshal(rstr_arg_buf, &rstr_arg);
        if (err != nil) {
            do_exit(err);
        }
        var start_time = float64(time.Now().Unix());
        obj, err := s3Client.GetObject(s3_arg["container"].(string),
                                       strconv.Itoa(int(rstr_arg["rstr_to_job_id"].(float64))),
                                       minio.GetObjectOptions{});
        if (err != nil) {
            do_exit(err);
        }
        data_file, err :=  os.Create("data/data");
        if (err != nil) {
            do_exit(err);
        }
        defer data_file.Close();
        if _, err = io.Copy(data_file, obj); err != nil {
            do_exit(err);
        }
        var fin_time = float64(time.Now().Unix());
        log.Println("successfully downloaded restore data.");
        /* update the start and finish time in  meta/arg.out file*/
        s3_arg["get_start"] = start_time;
        s3_arg["get_fin"] = fin_time;
        snpy_arg_out_buf, err := json.Marshal(snpy_arg);
        if (err != nil) {
            do_exit(err);
        }
        err = ioutil.WriteFile("meta/arg.out", snpy_arg_out_buf, 0600);
        if (err != nil) {
            do_exit(err);
        }
        do_exit(nil);
    } else {
        do_exit(errors.New("unimplemented meta/cmd."));
    }


}
